(ns dataset.core-test
  (:require [clojure.java.jdbc :as sql]
            [backport.clojure.core.reducers :as r])
  (:use clojure.test
        dataset.core
        dataset.sql))

(require 'dataset.core :reload)
(require 'dataset.sql :reload)


;; DB fixture

(def h2-spec 
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname "test"})

(defn bootstrap-db []
  (sql/with-db-connection [db-con h2-spec]

    ;; Accounts
    (sql/execute! db-con ["drop table accounts if exists"])
    (sql/execute! db-con 
                  ["create table accounts (mnemonic varchar, strategy varchar, strategydescription varchar)"])
    (sql/insert! db-con :accounts {:mnemonic "ACCSOV" :strategy "001" :strategydescription "Sovereigns"})
    (sql/insert! db-con :accounts {:mnemonic "ACCRUS" :strategy "002" :strategydescription "Russia"})
    (sql/insert! db-con :accounts {:mnemonic "ACCAFR" :strategy "002" :strategydescription "Africa"})

    ;; Rollups
    (sql/execute! db-con ["drop table hierarchy if exists"])
    (sql/execute! db-con ["create table hierarchy (strategy varchar, business varchar, subbusiness varchar)"])
    (sql/insert! db-con :hierarchy {:strategy "001" 
                                    :business "Flow Credit Trading" 
                                    :subbusiness "EMEA Flow Credit Trading"})
    (sql/insert! db-con :hierarchy {:strategy "002" 
                                    :business "EM Credit Trading" 
                                    :subbusiness "CEEMEA Credit Trading"})
    (sql/insert! db-con :hierarchy {:strategy "003" 
                                    :business "Structured Credit Trading" 
                                    :subbusiness "CDO Trading"})


    ;; Risk
    (sql/execute! db-con ["drop table risk if exists"])
    (sql/execute! db-con ["create table risk (tradeid int, pv double, cr01 double, ir01 double, theta double)"])
    (sql/insert! db-con :risk {:tradeid 1 :pv 1200 :cr01 3 :ir01 1.5 :theta 0.2})
    (sql/insert! db-con :risk {:tradeid 2 :pv -2000 :cr01 -4 :ir01 -1 :theta 0.3})
    
    ))

(defn query [sql]
  (sql/with-db-connection [db-con db-spec]
    (sql/query db-con [sql])))

(use-fixtures :once 
  (fn [f] 
    (bootstrap-db)
    (f)))

(deftest test-sql-query-conversion
  (let [sut (dataset.core.ApplicativeQueryable. 
             (dataset.sql.SQLQueryTransformer. #{"concat"}))]
    ;; primitives
    (is (= "1" (-parse-sexp sut '1)))
    (is (= "null" (-parse-sexp sut nil)))
    (is (= "'hello'" (-parse-sexp sut "hello")))

    ;; field names
    (is (= "greeting" (-parse-sexp sut 'greeting)))

    ;; operators
    (is (= "(1 + 2)" (-parse-sexp sut '(+ 1 2))))
    (is (= "(1 + 2 + 3)" (-parse-sexp sut '(+ 1 2 3))))
    (is (= "(name in ('john','jack'))"
           (-parse-sexp sut '(in :$name ["john" "jack"]))))
    (is (= "(code in (1,2,3))"
           (-parse-sexp sut '(in :$code [1 2 3]))))


    ;; functions
    (is (= "concat('hello','world')" (-parse-sexp sut '(concat "hello" "world"))))
    (is (= "concat(greeting,'world')" (-parse-sexp sut '(concat :$greeting "world"))))

    ;; garbage
    (is (= invalid (-parse-sexp sut '(garf "hello" "world"))))
    
    ;; combinations
    (is (= "(greeting = concat('hello','world'))"
           (-parse-sexp sut '(= :$greeting (concat "hello" "world")))))))


(defn to-vec [ds] (r/into [] ds))
(defn to-set [ds] (r/into #{} ds))

(deftest test-sql-datasource
  (let [accounts (sql-table->dataset h2-spec "accounts")
        hierarchy (sql-table->dataset h2-spec "hierarchy")
        accsov (sql-query->dataset h2-spec "select * from accounts where mnemonic = 'ACCSOV'")
        last-query (atom nil)
        prev dataset.sql/with-query-results]
    (binding [dataset.sql/with-query-results
              (fn [con q f] 
                (reset! last-query q)
                (println "Executing query:" q)
                (prev con q f))]
      
      ;; blanko select

      (is (= 3 (r/count accounts)))
      (is (= @last-query "select * from accounts"))

      ;; selects

      (is (= #{{:mnemonic "ACCSOV"} {:mnemonic "ACCRUS"} {:mnemonic "ACCAFR"}}
             (to-set (select accounts :$mnemonic))))
      
      (is (= #{"ACCSOV" "ACCRUS" "ACCAFR"}
             (to-set (r/map :mnem (select accounts [:$mnemonic :as :mnem])))))

      (is (= [{:mnemonic "ACCSOV" :strategy "001" :strategydescription "Sovereigns"}]
             (to-vec accsov)))

      (is (= [{:mnem "ACCSOV"}]
             (to-vec (select accsov [:$mnemonic :as :mnem]))))

      (is (= #{{:mnem "ACCSOV"} {:mnem "ACCRUS"} {:mnem "ACCAFR"}}
             (-> accounts 
                 (select [:$mnemonic :as :mnem] :$strategy)
                 (select :$mnem)
                 (to-set))))

      ;; filters

      (let [dataset (where accounts (= :$strategy "001"))]
        (is (instance? dataset.sql.SQLDataSet dataset))
        (is (= ["ACCSOV"]
               (to-vec (r/map :mnemonic dataset)))))

      (let [dataset (where accounts (or (= :$strategy "001") (= :$strategy "002")))]
        (is (instance? dataset.sql.SQLDataSet dataset))
        (is (= #{"ACCSOV" "ACCAFR" "ACCRUS"}
               (to-set (r/map :mnemonic dataset)))))

      (let [dataset (where accounts (in :$strategy #{"001" "002"}))]
        (is (instance? dataset.sql.SQLDataSet dataset))
        (is (= #{"ACCSOV" "ACCAFR" "ACCRUS"}
               (to-set (r/map :mnemonic dataset)))))

      (let [strategy-set #{"001" "002"}
            dataset (where accounts (in :$strategy strategy-set))]
        (is (instance? dataset.sql.SQLDataSet dataset))
        (is (= #{"ACCSOV" "ACCAFR" "ACCRUS"}
               (to-set (r/map :mnemonic dataset)))))

      (let [dataset (where (where accounts (= :$strategy "002"))
                           (= :$strategydescription "Africa"))]
        (is (instance? dataset.sql.SQLDataSet dataset))
        (is (= ["ACCAFR"]
               (to-vec (r/map :mnemonic dataset)))))

      (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] 
               ["ACCAFR" "EM Credit Trading"] [nil "Structured Credit Trading"]}
             (to-set (r/map (juxt :mnemonic :business) 
                                (join hierarchy accounts {:join-type :left} :$strategy)))))

      (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] 
               ["ACCAFR" "EM Credit Trading"] [nil "Structured Credit Trading"]}
             (to-set (r/map (juxt :mnemonic :business) 
                                (join accounts hierarchy {:join-type :right} :$strategy)))))


      ;; comment no outer join support for shit

      (is (= [{:mnemonic "ACCSOV" :strategy "001" :strategydescription "Sovereigns"}]
             (r/into [] (join [{:strategy "001"}] accounts {:join-flow :left} :$strategy))))
      (is (= @last-query "select * from accounts where (strategy in ('001'))"))

      (is (= [{:mnemonic "ACCSOV" :strategy "001" :strategydescription "Sovereigns"}]
             (r/into [] (join accounts [{:strategy "001"}] {:join-flow :right} :$strategy))))
      (is (= @last-query "select * from accounts where (strategy in ('001'))"))


      ;; joins

      (let [joindata (join accounts hierarchy {} :$strategy)]
        (is (instance? dataset.sql.SQLDataSet joindata))
        (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] ["ACCAFR" "EM Credit Trading"]}
               (to-set (r/map (juxt :mnemonic :business) joindata)))))

      ;; Non-SQL interactions
      (is (= #{"SOV" "RUS" "AFR"}
             (to-set (r/map :mnem (select accounts [(subs :$mnemonic 3) :as :mnem])))))

      (let [joindata (join accounts (cache hierarchy) {} :$strategy)]
        (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] ["ACCAFR" "EM Credit Trading"]}
               (to-set (r/map (juxt :mnemonic :business) joindata))))))))


(deftest test-clojure-datasource
  (let [data (->clojure-dataset [{:mnemonic "ACCSOV"}
                                 {:mnemonic "ACCRUS"}
                                 {:mnemonic "ACCAFR"}])]
    (is (= #{{:mnem "SOV"} {:mnem "RUS"} {:mnem "AFR"}}
           (to-set (select data [(subs :$mnemonic 3) :as :mnem]))))

    (is (= ["ACCSOV"]
           (to-vec (r/map :mnemonic (where data (.contains :$mnemonic "SOV"))))))))


(deftest test-clojure-joins
  (let [accounts (cache (sql-table->dataset h2-spec "accounts"))
        hierarchy (cache (sql-table->dataset h2-spec "hierarchy"))]

    (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] ["ACCAFR" "EM Credit Trading"]}
         (to-set (r/map (juxt :mnemonic :business) 
                            (join accounts hierarchy {} :$strategy)))))

    (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] ["ACCAFR" "EM Credit Trading"]
             [nil "Structured Credit Trading"]}
           (to-set (r/map (juxt :mnemonic :business) 
                              (join hierarchy accounts {:join-type :left} :$strategy)))))

    (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] ["ACCAFR" "EM Credit Trading"]
             [nil "Structured Credit Trading"]}
           (to-set (r/map (juxt :mnemonic :business) 
                              (join accounts hierarchy {:join-type :right} :$strategy)))))

    (is (= #{["ACCSOV" "Flow Credit Trading"] ["ACCRUS" "EM Credit Trading"] ["ACCAFR" "EM Credit Trading"]
             [nil "Structured Credit Trading"]}
           (to-set (r/map (juxt :mnemonic :business) 
                              (join accounts hierarchy {:join-type :outer} :$strategy)))))

    (is (= 9 (r/count (join accounts hierarchy {:join-type :cross}))))

    (is (= [{:a 1 :b 2 :c 3}]
           (to-vec (join [{:a 1 :b 2}] [{:a 4 :b 2 :c 3}] {} :$b))))))
