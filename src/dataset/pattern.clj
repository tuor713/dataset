(ns dataset.pattern
  "Utilities for handling data sources that support only limited query patterns such 
as RESTful web services"
  (:require [backport.clojure.core.reducers :as r]
            [backport.clojure.core.protocols :as p]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:use dataset.core
        dataset.query))

(defn trade-data
  [business region desk]
  (let [path (str "test/data/trades/" business "/" region "/" desk "/data.edn")]
    (when (.isFile (io/file path))
      (-> path slurp read-string))))

(defn- trade-dataset-impl [where-clauses]
  ;; Don't implement Selectable, this should delegate to the Clojure source because we are not
  ;; supporting any actual remote execution
  ;; However, there is a problem here in that all the wrapping we have done so far makes it hard if not
  ;; impossible to transmute the order of select and where clauses. For single data sources that is not so much
  ;; an issue as the programmer could get it right in the code himself.
  ;; However, for joins, particular with join-strategies where clauses will be generated under the covers and should
  ;; propagate properly as low as possible. Again this probably a concern that needs to be added to the default
  ;; ClojureDataSet
  (reify
    dataset.core.Filterable
    (-where [self conditions]
      (let [conds (map #(-> [% (-parse-sexp (dataset.query.SimpleQueryable. #{:$business :$region :$desk} #{'=}) %)]) conditions)
            supported (remove #{invalid} (map second conds))
            unsupported (map first (filter #(= (second %) invalid) conds))
            inner (if (seq supported)
                    (trade-dataset-impl (concat where-clauses supported))
                    self)]
        (if (seq unsupported)
          (where-wrapper inner unsupported)
          inner)))

    p/CollReduce
    (coll-reduce [_ f]
      (p/coll-reduce (trade-data "credit" "europe" "flow") f))
    (coll-reduce [_ f val]
      (p/coll-reduce (trade-data "credit" "europe" "flow") f val))))

(defn trade-dataset [] (trade-dataset-impl []))
