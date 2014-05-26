(ns dataset.sql
  (:require [backport.clojure.core.reducers :as r]
            [backport.clojure.core.protocols :as p]
            [clojure.string :as s]
            [clojure.java.jdbc :as sql])
  (:use dataset.core
        dataset.query
        [clojure.set :only [union intersection difference]]))

;; SQL library adapters

;; also a testing schim
(defn ^:dynamic with-query-results [con q f]
  (sql/db-query-with-resultset con [q] (comp f resultset-seq)))


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
                     (s/join "," (map (fn [[label selector]] (str selector " as " (field->field-name label)))
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
    (-parse-sexp (dataset.query.ApplicativeQueryable. (SQLQueryTransformer. #{"concat"})) sexp))

  Selectable
  (-select [self fields]
    (let [parsed-fields (map (fn [[k exp]]
                               (let [parsed (-parse-sexp self exp)]
                                 [(if (= parsed invalid) :unsupported :supported)
                                  k
                                  (if (= parsed invalid) exp parsed)]))
                          fields)
          {supported :supported unsupported :unsupported} (group-by first parsed-fields)

          ;; need to ensure dependencies for calculations of unsupported fields are passed through
          ;; as well as calculation in native layer are passed through in later enrichment
          unsupported-dependencies
          (set (mapcat (comp field-refs last) unsupported))

          inner (if (seq supported)
                  (SQLDataSet. spec (sql-select fragment
                                      (concat
                                        (map rest supported)
                                        (map
                                          (fn [f] [f (-parse-sexp self f)])
                                          unsupported-dependencies))))
                  self)]
      (if (seq unsupported)
        (select-wrapper inner
          (concat
            (map rest unsupported)
            (map (fn [[_ f _]] [f f]) supported)))
        inner)))

  Filterable
  (-where [self conditions]
    (let [parsed-conditions
          (map (fn [exp]
                 (let [parsed (-parse-sexp self exp)]
                   [(if (= parsed invalid) :unsupported :supported)
                    (if (= parsed invalid) exp parsed)]))
            conditions)

          {supported :supported unsupported :unsupported} (group-by first parsed-conditions)


          inner (if (seq supported)
                  (SQLDataSet. spec (sql-where fragment (map second supported)))
                  self)]
      (if (seq unsupported)
        (where-wrapper inner (map second unsupported))
        inner)))

  Joinable
  (-join [self other options fields]
    ;; (instance? SQLDataSet other), fails on 1.2.1 for no good reason
    (if (and (= (class self) (class other))
          (= spec (db-spec other)))
      (SQLDataSet.
       spec
       (SQLJoin. fragment
                 (db-fragment other)
                 (:join-type options)
                 fields))
      (join-wrapper self other options fields)))

  p/CollReduce
  (coll-reduce
    [_ f]
    (sql/with-db-connection [con spec]
      (with-query-results
        con
        (sql-query fragment (:connection con))
        #(r/reduce f %))))
  (coll-reduce
    [_ f val]
    (sql/with-db-connection [con spec]
      (with-query-results
        con
        (sql-query fragment (:connection con))
        #(r/reduce f val %)))))

(defn sql-table->dataset
  [db-spec table]
  (SQLDataSet. db-spec (SQLDelegate. table [] [])))

(defn sql-query->dataset
  "Create an SQL dataset from a given query. Additional select, where, joins etc
  will be wrapped around the query."
  [db-spec query]
  (SQLDataSet. db-spec (SQLQuery. query)))
