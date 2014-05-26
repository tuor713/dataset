# dataset - core.reducers based datasets for disparate data sources

A Clojure library designed to handle datasets from disparate sources. This library is meant for the kind of environments where some data is real data warehouses (SQL databases), some data in caches, some data may be even in flat feed files. However, for
analysis purposes it is necessary to create joins and calculated functions across that data.


Included with the batteries is Clojure 1.2+ backport of clojure.core.reducers (because they are awesome and not everyone is on 1.5+).

## What does usage look like?

The core library is structured around dataset definitions (like core clojure dataset,
SQL datasets, CSV datasets, etc) and operations (defined primarily as macros): select, where, join, order. At the minimum each dataset implements CollReduce plus potentially
richer dataset operatiors.

```clojure
(require '[dataset.core :as q]
         '[dataset.sql :as sql]
         '[dataset.csv :as csv]
         '[backport.clojure.core.reducers :as r])


;; some data of title,author
(def books (csv/csv-dataset "test/collection.csv"))

;; a database of book meta data: title, author, genre, etc
(def books-db {...})
(def books (sql/sql-table->dataset books-db "books"))

;; a literal dataset
(def weightings
  [{:genre "Fantasy" :weight 80}
   {:genre "Computer Science" :weight 70}
   {:genre "Horror" :weight 10}
   {:genre "Romance" :weight 50}
   {:genre "Science Fiction" :weight 40}
   {:genre "History" :weight 60}])

(defn ->vec [ds] (r/into [] ds))

;; running a multi join query
(-> books
    (q/join books {:join-flow :left} :$title)
    (q/join weightings {} :$genre)
    (q/select :$title, :$author, :$genre, :$weight)
    (q/where (> :$weight 50))
    (->vec))
```


## Design notes

See [part 1](http://tuor713.github.io/dataset/index.html) and [part 2](http://tuor713.github.io/dataset2/index.html).
