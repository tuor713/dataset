(ns dataset.sql
  (:require [backport.clojure.core.reducers :as r]
            [backport.clojure.core.protocols :as p]
            [clojure.java.jdbc :as sql]
            [clojure.string :as s])
  (:use dataset.core
        [clojure.set :only [union intersection difference]]))

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
  (db-fragment [self]))



(defprotocol SQLFragment
  (sql-query [self connection])
  (sql-select [self projections])
  (sql-where [self conditions]))

(defrecord SQLDelegate [source projections conditions]
  SQLFragment
  (sql-query [self con]
    (str "select " (if (seq projections) 
                     (s/join "," (map (fn [[label selector]] (str selector " as " (name label))) 
                                      projections))
                     "*")
         " from " (if (string? source) source (str "(" (sql-query source con) ")"))
         (when (seq conditions)
           (str " where " (s/join " and " conditions)))))
  (sql-select [self projs]
    (if (seq projections)
      (SQLDelegate. self projs [])
      (SQLDelegate. source projs conditions)))
  (sql-where [self conds]
    (SQLDelegate. source projections (into conditions conds))))

(defrecord SQLQuery [query]
  SQLFragment
  (sql-query [self _] query)
  (sql-select [self projections] (SQLDelegate. self projections []))
  (sql-where [self conditions] (SQLDelegate. self [] conditions)))

(defn- query->columns [con query]
  (with-open [stmt (.prepareStatement con query)]
    (let [rs-meta (.getMetaData stmt)]
      (doall (map #(.getColumnName rs-meta %) (range 1 (inc (.getColumnCount rs-meta))))))))


;; Is there a better way to do joins beside querying the result set metadata (presumes an actual query
;; and parse step on the backend)
(defrecord SQLJoin [left right join-type fields]
  SQLFragment
  (sql-query [self con]
    (let [q1 (sql-query left con)
          q2 (sql-query right con)
          
          common-join-cols (set (keep (fn [[l r]] (when (= l r) (s/lower-case (field->field-name l)))) fields))

          left-columns (set (query->columns con q1))
          right-columns (set (query->columns con q2))]
      (str "select " 
           (s/join ", "
                   (concat (difference left-columns right-columns)
                           (difference right-columns left-columns)
                           (map 
                            #(if (contains? common-join-cols (s/lower-case %)) 
                               (str (if (= join-type :right) "rhs." "lhs.") %)
                               (str "coalesce(lhs." % ", rhs." % ") as " %))
                            (intersection left-columns right-columns))))
           " from (" q1 ") lhs"
           " " (name join-type) " join (" q2 ") rhs"
           " on (" 
           (s/join " and " (map 
                            (fn [[l r]] (str "lhs." (field->field-name l) 
                                             " = "
                                             "rhs." (field->field-name r)))
                            fields))
           ")")))
  (sql-select [self projections] (SQLDelegate. self projections []))
  (sql-where [self conditions] (SQLDelegate. self [] conditions)))

(deftype SQLDataSet [spec fragment]
  clojure.lang.Seqable
  (seq [self] (seq (r/into [] self)))

  SQLAttributes
  (db-spec [_] spec)
  (db-fragment [_] fragment)

  Queryable
  (-parse-sexp [self sexp]
    (-parse-sexp (dataset.core.ApplicativeQueryable. (SQLQueryTransformer. #{"concat"})) sexp))

  Selectable
  (-select [self fields]
    (SQLDataSet. spec (sql-select fragment fields)))

  Filterable
  (-where [self conditions]
    (SQLDataSet. spec (sql-where fragment conditions)))

  Joinable
  (-join [self other options fields]
    (if (and (instance? SQLDataSet other) (= spec (db-spec other)))
      (SQLDataSet.
       spec
       (SQLJoin. fragment 
                 (db-fragment other)
                 (:join-type options)
                 fields))
      (-join (->clojure-dataset self) other options fields)))

  p/CollReduce
  (coll-reduce 
    [_ f]
    (sql/with-db-connection [con spec]
      (let [q (sql-query fragment (:connection con))]
        (println "Executing query:" q)
        (sql/db-query-with-resultset con [q]
          (fn [res] (r/reduce f (resultset-seq res)))))))
  (coll-reduce 
    [_ f val]
    (sql/with-db-connection [con spec]
      (let [q (sql-query fragment (:connection con))]
        (println "Executing query:" q)
        (sql/db-query-with-resultset con [q]
          (fn [res] (r/reduce f val (resultset-seq res))))))))

(defn sql-table->dataset
  [db-spec table]
  (SQLDataSet. db-spec (SQLDelegate. table [] [])))

(defn sql-query->dataset
  [db-spec query]
  (SQLDataSet. db-spec (SQLQuery. query)))






