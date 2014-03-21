(ns dataset.core
  "Dataset library built on top of (backported) reducers library."
  (:require [backport.clojure.core.reducers :as r]
            [backport.clojure.core.protocols :as p]
            [clojure.string :as s])
  (:use [clojure.set :only [union intersection difference rename-keys]]
        [clojure.walk :only [postwalk]]))


(comment "Likely API usage"

         (join ds ds2
               {:join-type :left}
               [field1 field2])


         (-> (join risk [:Mnemonic :Mnemonic])
             (left-join risk [:Mnemonic :Mnemonic])
             ))

;; Foundational protocols

;; Let's separate:
;; 1. Macro API
;; 2. Functional API for data sources (Clojure methods handling the all around implementation)
;; 3. Protocols used by functional API 
;; 4. Implementations of these protocols for various actual sources

(declare ->clojure-dataset)

;; 3. Data source protocols

(defprotocol SexpQueryTransformer
  (-to-value [self primitive])
  (-to-field [self field-name])
  (-operator? [self opname])
  (-function? [self fname]))

(defprotocol Queryable 
  (-parse-sexp [self sexp]))

(defprotocol Schematic
  (-fields [self] "Returns the set of fields for the datasource"))

(defprotocol Selectable
  (-select [self fields]))

(defprotocol Filterable
  (-where [self conditions]))

(defprotocol Joinable
  (-join [self other hints join-fields]))

;; 2. Functional API


(defn field-ref? [kw]
  (and (keyword? kw) (.startsWith (name kw) "$")))

(defn field->field-name [kw]
  (if (field-ref? kw) 
    (subs (name kw) 1) 
    (name kw)))

(defn field->keyword [kv]
  (keyword (field->field-name kv)))

(defn select* [source & fields]
  (if (satisfies? Selectable source)
    (let [converted (map (fn [[exp _ label]] [exp (-parse-sexp source exp) label]) 
                         (map (fn [f] (if (keyword? f) [f :as (field->keyword f)] f)) fields))
          handled-fields (map (juxt #(nth % 2) second)
                              (remove #(= (second %) ::invalid) converted))
          unhandled-fields (map (juxt first (constantly :as) #(nth % 2)) 
                                (filter #(= (second %) ::invalid) converted))
          with-handled (if (seq handled-fields)
                         (-select source handled-fields)
                         source)]
      (if (seq unhandled-fields)
        (apply select* (->clojure-dataset with-handled) unhandled-fields)
        with-handled))
    (apply select* (->clojure-dataset source) fields)))


;; A clojure wrapper means (unless we keep track of fields specially) that
;; we can no longer run queries on the original, i.e. we only get best
;; performance if we can safely reorder query steps
(defn where* [source & conditions]
  (if (satisfies? Filterable source)
    (let [converted (map #(-> [% (-parse-sexp source %)]) conditions)
          handled-conditions (remove #{::invalid} (keep second converted))
          unhandled-conditions (keep #(when (= ::invalid (second %)) (first %)) converted)
          with-handled (if (seq handled-conditions)
                         (-where source handled-conditions)
                         source)]
      (if (seq unhandled-conditions)
        (apply where* (->clojure-dataset with-handled) unhandled-conditions)
        with-handled))
    (apply where* (->clojure-dataset source) conditions)))

(defn join* 
  "Supported options:
- join-type: :left, :right, :inner (default), :outer
- join-flow: :left, :right, :none, :auto (default). Note combinations like join-type=left, join-flow=right are invalid."
  [left right 
   options
   & fields]
  (let [params (merge {:join-type :left :join-flow :auto} options)
        join-type (:join-type params)
        join-flow (:join-flow params)]
    (when (= #{:left :right} (set [join-type join-flow]))
      (throw (IllegalArgumentException. 
              (str "Illegal combination of join-type " join-type " and join-flow " join-flow))))
    
    (let [sanitized-fields (map #(if (sequential? %) % [% %]) fields)]
      (cond
       ;; standardize right joins to left joins to reduce duplicate implementation further down
       (= :right join-type)
       (apply join* 
              right left 
              (assoc params
                :join-type (get {:left :right :right :left} join-type join-type)
                :join-flow (get {:left :right :right :left} join-type join-type))
              (map #(-> [(second %) (first %)]) sanitized-fields))

       (satisfies? Joinable left)
       (-join left right params sanitized-fields)

       :else
       (apply join* (->clojure-dataset left) right params sanitized-fields)))))

;; 1. Macro API

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

(defmacro select [source & fields]
  `(select* ~source 
            ~@(map 
               (fn [f] (if (vector? f)
                         (vec (cons (list 'quote-with-code (first f)) (rest f)))
                         (list 'quote-with-code f))) 
               fields)))

(defmacro where [source & conditions]
  `(where* ~source ~@(map #(list 'quote-with-code %) conditions)))


(defmacro join [left right options & fields]
  `(join* ~left ~right ~options ~@fields))


;; 4. Implementation - Query parsing

(def invalid ::invalid)

(deftype ApplicativeQueryable [transformer]
  Queryable
  (-parse-sexp [self sexp]
    (cond
     (list? sexp) 
     (let [args (map #(-parse-sexp self %) (rest sexp))
           invalid? (some #{invalid} args)]
       (cond
        invalid? invalid
        (-operator? transformer (str (first sexp))) (str "(" (s/join (str " " (first sexp) " ") args) ")")
        (-function? transformer (str (first sexp))) (str (str (first sexp)) "(" (s/join "," args) ")")
        :else invalid))

     (or (set? sexp) (sequential? sexp))
     (str "(" (s/join "," (map #(-parse-sexp self %) sexp)) ")")

     (field-ref? sexp)
     (-to-field transformer (field->field-name sexp))

     :else (-to-value transformer sexp))))


;; 4. Implementation - Clojure

(defn in [v values]
  (contains? (set values) v))

(deftype ClojureDataSet [reducible]
  clojure.lang.Seqable
  (seq [self] 
    (seq (r/into [] self)))

  Selectable
  (-select [self fields]
    (ClojureDataSet.
     (r/map
      (fn [rec]
        (persistent!
         (reduce
          (fn [res [key f]] (assoc! res key (f rec)))
          (transient {})
          fields)))
      self)))

  Filterable
  (-where [self conditions]
    (ClojureDataSet.
     (r/filter
      (fn [rec] (every? #(% rec) conditions))
      self)))

  Queryable
  (-parse-sexp [self sexp]
    (or (:function (meta sexp)) 
        (if (field-ref? sexp) 
          (field->keyword sexp)
          sexp)))

  Joinable
  (-join [self other options fields]
    (ClojureDataSet.
     (lazy-seq
      (let [lhskey (apply juxt (map (comp field->keyword first) fields))
            rhskey (apply juxt (map (comp field->keyword second) fields))

            lhs-groups (r/group-by lhskey self)
            rhs-groups (r/group-by rhskey other)

            result-keys
            (case (:join-type options)
              :left (keys lhs-groups)
              :inner (intersection (set (keys lhs-groups)) (set (keys rhs-groups)))
              :outer (union (set (keys lhs-groups)) (set (keys rhs-groups))))]
        (for [k result-keys
              l (lhs-groups k)
              r (rhs-groups k)]
          (merge l r))))))

  p/CollReduce
  (coll-reduce [_ f]
    (p/coll-reduce reducible f))
  (coll-reduce [_ f val]
    (p/coll-reduce reducible f val)))

(defn ->clojure-dataset
  "Wrap a source in a Clojure source, this does not realize the dataset (as cache would do). 
Instead all further operation are simply proxied on the Clojure source."
  [source]
  (ClojureDataSet. source))


;; Misc utilities

(defn label [dataset namespace]
  (->clojure-dataset
   (r/map 
    (fn [rec]
      (into {}
            (map 
             (fn [e] [(keyword (name namespace) (name (key e))) (val e)])
             rec)))
    dataset)))

(defn cache [dataset]
  (->clojure-dataset (r/into [] dataset)))



