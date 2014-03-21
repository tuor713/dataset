(ns dataset.core
  "Dataset library built on top of (backported) reducers library."
  (:require [backport.clojure.core.reducers :as r]
            [backport.clojure.core.protocols :as p]
            [clojure.java.jdbc :as sql]
            [clojure.string :as s])
  (:use [clojure.set :only [union intersection rename-keys]]
        [clojure.walk :only [postwalk]]))


(comment "Likely API usage"
         (defsource accounts ... definition ...)

         "Functional usage"
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

;; Usually should be implemented by delegating to an appropriate SexpQuery object
;; So a data source would implement Queryable and then construct such an object via the 
;; use of the default parser against a custom SexpQueryTransformer
(defprotocol Queryable 
  (-parse-sexp [self sexp]))

(defprotocol Schematic
  (-fields [self] "Returns the set of fields for the datasource"))

(defprotocol Selectable
  (-select [self fields]))

(defprotocol Filterable
  (-where [self conditions]))

(defprotocol Joinable
  (-join [self hints join-fields]))

;; 2. Functional API


(defn field-ref? [kw]
  (and (keyword? kw) (.startsWith (name kw) "$")))

(defn field->field-name [kw]
  (if (field-ref? kw) 
    (subs (name kw) 1) 
    (name kw)))

(defn select* [source & fields]
  (if (satisfies? Selectable source)
    (let [converted (map (fn [[exp _ label]] [exp (-parse-sexp source exp) label]) 
                         (map (fn [f] (if (keyword? f) [f :as (keyword (field->field-name f))] f)) fields))
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



;; 1. Macro API

(defmacro quote-with-code [sexp]
  (if (instance? clojure.lang.IObj sexp)
    `(with-meta (quote ~sexp)
       {:function ~(let [s (gensym)]
                     (list
                      'fn
                      [s]
                      (postwalk 
                       (fn [exp]
                         (if (field-ref? exp)
                           (list (keyword (field->field-name exp)) s)
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

;; 4. Implementation - Query parsing

(def invalid ::invalid)

(deftype SQLQueryTransformer [functions]
  SexpQueryTransformer
  (-to-value [self primitive] 
    (cond
     (nil? primitive) "null"
     (string? primitive) (str "'" primitive "'")
     :else (str primitive)))

  (-to-field [self field-name] field-name)

  (-operator? [self opname] (contains? #{"+" "-" "*" "/"
                                         "<" "<=" ">" ">=" "="
                                         "in" "or" "and"}
                                       opname))

  (-function? [self fname] (contains? functions fname)))

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
          (keyword (field->field-name sexp))
          sexp)))

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


;; 4. Implementation - SQL database

(defn- to-query [attrs]
  (str "select " (if (seq (:fields attrs))
                   (s/join "," (map (fn [[label selector]] (str selector " as " (name label))) 
                                    (:fields attrs)))
                   "*")
       " from " 
       (if (not (s/blank? (:query attrs)))
         (str "(" (:query attrs) ")")
         (:table attrs))

       (when (seq (:filters attrs))
         (str " where "
              (s/join " and " (:filters attrs))))))

(defn- to-sql-value [spec]
  (if (keyword? spec)
    (name spec)
    nil))

(defprotocol SQLAttributes
  (db-spec [self])
  (db-attrs [self]))

(deftype SQLDataSet [spec attrs]
  clojure.lang.Seqable
  (seq [self] (seq (r/into [] self)))

  SQLAttributes
  (db-spec [_] spec)
  (db-attrs [_] attrs)

  Queryable
  (-parse-sexp [self sexp]
    (-parse-sexp (ApplicativeQueryable. (SQLQueryTransformer. #{"concat"})) sexp))

  Selectable
  (-select [self fields]
    (SQLDataSet. spec 
                 (if (contains? attrs :fields)
                   {:query (to-query attrs) :fields (into {} fields)}
                   (assoc attrs :fields (into {} fields)))))

  Filterable
  (-where [self conditions]
    (SQLDataSet. spec (update-in attrs [:filters] #(into (or % []) conditions))))

  p/CollReduce
  (coll-reduce 
    [_ f]
    (sql/with-db-connection [con spec]
      (let [q (to-query attrs)]
        (println "Executing query:" q)
        (sql/db-query-with-resultset con [q]
          (fn [res] (r/reduce f (resultset-seq res)))))))
  (coll-reduce 
    [_ f val]
    (sql/with-db-connection [con spec]
      (let [q (to-query attrs)]
        (println "Executing query:" q)
        (sql/db-query-with-resultset con [q]
          (fn [res] (r/reduce f val (resultset-seq res))))))))

(defn sql-table->dataset
  [db-spec table]
  (SQLDataSet. db-spec {:table table}))

(defn sql-query->dataset
  [db-spec query]
  (SQLDataSet. db-spec {:query query}))





;; TODO:
;; - this is actually quite interesting in that if we go down to the reducers API we loose potential
;;   optimizations of joins
(defn label [dataset namespace]
  ;; TODO optimize for schematic datasets
  (r/map 
   (fn [rec]
     (into {}
           (map 
            (fn [e] [(keyword (name namespace) (name (key e))) (val e)])
            rec)))
   dataset))

(defn cache [dataset]
  (r/into [] dataset))


;; Additional protocols

(defn join [lhs rhs lhskey rhskey join-type]
  (let [lhs-groups (r/group-by lhskey lhs)
        rhs-groups (r/group-by rhskey rhs)
        result-keys (join-type (set (keys lhs-groups)) (set (keys rhs-groups)))]
    (for [k result-keys
          l (lhs-groups k)
          r (rhs-groups k)]
      (merge l r))))

(def inner-join #(join %1 %2 %3 %4 intersection))
(def outer-join #(join %1 %2 %3 %4 union))
(def left-join #(join %1 %2 %3 %3 (fn [l _] l)))
(def right-join #(left-join %2 %1 %4 %3))

