(ns dataset.csv
  (:require [backport.clojure.core.protocols :as p]
            [backport.clojure.core.reducers :as r])
  (:use dataset.core
        [clojure.java.io :only [reader]]
        [clojure.data.csv :only [read-csv]]))

(defn- csv-records [r sep]
  (let [[h & rs] (read-csv r :separator sep)
        s (apply create-struct (map keyword h))]
    (map #(apply struct s %) rs)))

(defn csv-dataset 
  ([file] (csv-dataset file \,))
  ([file separator]
     (reify
       clojure.lang.Seqable
       (seq [self] (seq (r/into [] self)))

       p/CollReduce
       (coll-reduce [_ f]
         (with-open [r (reader file)]
           (r/reduce f (csv-records r separator))))
       (coll-reduce [_ f val]
         (with-open [r (reader file)]
           (r/reduce f val (csv-records r separator)))))))

