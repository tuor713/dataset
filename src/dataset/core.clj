(ns dataset.core
  (:require [backport.clojure.core.reducers :as r]
            [backport.clojure.core.protocols :as p]
            [clojure.string :as s]
            [clojure.set :as set])
  (:use [clojure.walk :only [postwalk]]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Data source protocols ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol Selectable
  (-select [self fields] "A list of fields as [<field label> <exp>] pairs"))

(defprotocol Filterable
  (-where [self conditions]))

(defprotocol Joinable
  (-join [self other hints join-fields]))

(defprotocol Orderable
  (-order [self sexps]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query parsing utilities ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn field-ref? [kw]
  (and (keyword? kw) (.startsWith (name kw) "$")))

(defn field->field-name [kw]
  (if (field-ref? kw)
    (subs (name kw) 1)
    (name kw)))

(defn field->keyword [kv]
  (keyword (field->field-name kv)))

(defn field-refs [form]
  (cond
    (or (list? form) (instance? clojure.lang.IMapEntry form) (seq? form) (coll? form))
    (set (mapcat field-refs form))

    (field-ref? form)
    [form]

    :else nil))

(defn- function-quote [sexp]
  (postwalk
    (fn [exp]
      (if (list? exp)
        `(list (quote ~(first exp)) ~@(rest exp))
        exp))
    sexp))

(defmacro quote-with-code [sexp]
  (if (instance? clojure.lang.IObj sexp)
    `(with-meta ~(function-quote sexp)
       {:function ~(let [s (gensym)]
                     (list
                       'fn
                       [s]
                       (postwalk
                         (fn [exp]
                           (if (field-ref? exp)
                             (list (field->keyword exp) s)
                             exp))
                         sexp)))})
    sexp))


;;;;;;;;;;;;;;;;;;;;
;; Functional API ;;
;;;;;;;;;;;;;;;;;;;;

(declare where-wrapper select-wrapper order-wrapper join-wrapper in)

(defn select* [source fields]
  (if (satisfies? Selectable source)
    (-select source fields)
    (select-wrapper source fields)))

(defn where* [source conditions]
  (if (satisfies? Filterable source)
    (-where source conditions)
    (where-wrapper source conditions)))

(defn order* [source expressions]
  (if (satisfies? Orderable source)
    (-order source expressions)
    (order-wrapper source expressions)))

(declare join*)

(defn- handle-join-flow [left right flow options fields]
  (let [right? (= flow :right)
        extractor (apply juxt (map (comp field->keyword (if right? second first)) fields))
        combinations (r/reduce (fn [res rec]
                                 (map conj res (extractor rec)))
                       (repeat (count fields) #{})
                       (if right? right left))

        sink (where* (if right? left right)
               (map
                 (fn [f selections]
                   (quote-with-code (in f selections)))
                 (map (if right? first second) fields)
                 combinations))]
    (join* (if right? sink left) (if right? right sink) (assoc options :join-flow :none) fields)))

(defn join*
  "Supported options:
- join-type: :left, :right, :inner (default), :outer
- join-flow: :left, :right, :none, :auto (default). Note combinations like join-type=left, join-flow=right are invalid."
  [left right
   options
   fields]
  (let [params (merge {:join-type :inner :join-flow :auto} options)
        join-type (:join-type params)
        join-flow (:join-flow params)]
    (when (= #{:left :right} (set [join-type join-flow]))
      (throw (IllegalArgumentException.
               (str "Illegal combination of join-type " join-type " and join-flow " join-flow))))

    (let [sanitized-fields (map #(if (sequential? %) % [% %]) fields)]
      (cond
        (= join-flow :left) (handle-join-flow left right join-flow options sanitized-fields)
        (= join-flow :right) (handle-join-flow left right join-flow options sanitized-fields)

        (satisfies? Joinable left) (-join left right params sanitized-fields)
        :else (join-wrapper left right params sanitized-fields)))))

;;;;;;;;;;;;;;;;;;;;;;
;; Macro / User API ;;
;;;;;;;;;;;;;;;;;;;;;;

(defn in [v values]
  (contains? (set values) v))

(defmacro select
  "Sample usage: (select <source> :$field [<exp> :as :$otherfield])"
  [source & fields]
  `(select* ~source
     ~(vec (map
             (fn [f] (if (vector? f)
                       [(nth f 2) (list 'quote-with-code (first f))]
                       [f (list 'quote-with-code f)]))
             fields))))

(defmacro where [source & conditions]
  `(where* ~source ~(vec (map #(list 'quote-with-code %) conditions))))

(defmacro order [source & sexps]
  `(order* ~source ~(vec (map #(list 'quote-with-code %) sexps))))

(defn join [left right options & fields]
  (join* left right options fields))


(defn lazy-cache [dataset]
  (lazy-seq (r/into [] dataset)))

(defn cache [dataset]
  (r/into [] dataset))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Implementation - Clojure ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- sexp->fn [sexp]
  (or (:function (meta sexp))
    (if (field-ref? sexp)
      (field->keyword sexp)
      sexp)))


;; No support for where reordering. If there are non-native where clauses in the chain
;; they will have to be manually reordered for now.
;; The alternative of weight based path optimization transforms this from a library to an engine
;; and takes away predictability, which is not desired.
(defn where-wrapper [source conditions]
  (let [parsed-conditions (map sexp->fn conditions)]
    (r/filter
      (fn [rec] (every? #(% rec) parsed-conditions))
      source)))

(defn select-wrapper [source fields]
  (let [;; to allow some push behaviour we want to exclude one-to-one mappings
        output-fields (set (keep (fn [[k exp]] (when-not (= k exp) k)) fields))
        parsed-fields (map (fn [[k sexp]] [(field->keyword k) (sexp->fn sexp)]) fields)
        mapped-source
        (r/map
          (fn [rec]
            (persistent!
              (reduce
                (fn [res [key f]] (assoc! res key (f rec)))
                (transient {})
                parsed-fields)))
          source)]
    (reify
      Filterable
      (-where [self conditions]
        (let [{pushable true unpushable false}
              (group-by #(empty? (set/intersection (set (field-refs %)) output-fields)) conditions)
              inner (if (seq pushable) (where* source pushable) source)]
          (where-wrapper
            (select-wrapper inner fields)
            unpushable)))

      p/CollReduce
      (coll-reduce [_ f]
        (p/coll-reduce mapped-source f))
      (coll-reduce [_ f val]
        (p/coll-reduce mapped-source f val)))))

(defn order-wrapper [source sexps]
  (let [fs (map sexp->fn sexps)]
    (sort-by
      (fn [rec] (vec (map #(% rec) fs)))
      (r/into [] source))))


(defn- rec-join [lhskey rhskey lhs rhs join-type]
  (let [left-side? (or (= :outer join-type) (= :left join-type))
        right-side? (or (= :outer join-type) (= :right join-type))]
    (cond
      (not lhs) (when right-side? (apply concat rhs))
      (not rhs) (when left-side? (apply concat lhs))

      :else
      (let [glhs (first lhs)
            grhs (first rhs)
            c (compare (lhskey (first glhs)) (rhskey (first grhs)))]
        (cond
          (= c 0)
          (lazy-cat (for [l glhs r grhs] (merge r l))
            (rec-join lhskey rhskey (next lhs) (next rhs) join-type))

          (< c 0)
          (lazy-cat (when left-side? glhs)
            (rec-join lhskey rhskey (next lhs) rhs join-type))

          (> c 0)
          (lazy-cat (when right-side? grhs)
            (rec-join lhskey rhskey lhs (next rhs) join-type)))))))

(defn- clojure-join [lhskey rhskey lhs rhs join-type]
  (rec-join
    lhskey
    rhskey
    (seq (partition-by lhskey lhs))
    (seq (partition-by rhskey rhs))
    join-type))

(defn join-wrapper [left right options fields]
  (if (= (:join-type options) :cross)
    ;; no caching here so that we make no assumptions on whether any of these
    ;; datasets will fit into memory, users can pre-cache both datasets before
    ;; passing them in the join if desired
    (r/mapcat (fn [rec] (r/into [] (r/map #(merge rec %) right))) left)

    ;; sort and merge algorithm since we are already dealing with in-memory datasets at this point
    ;; Sorting is in theory N logN as compared to hashing which should be N
    (lazy-seq
      (let [lhskey (apply juxt (map (comp field->keyword first) fields))
            rhskey (apply juxt (map (comp field->keyword second) fields))

            lhssorted (sort-by lhskey (r/into [] left))
            rhssorted (sort-by rhskey (r/into [] right))]
        (clojure-join lhskey rhskey lhssorted rhssorted (:join-type options))))))

;; end