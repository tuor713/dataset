(ns dataset.core-test
  (:require [clojure.java.jdbc :as sql]
            [backport.clojure.core.reducers :as r])
  (:use clojure.test
        dataset.core))


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
    (sql/insert! db-con :hierarchy {:strategy "002" 
                                    :business "EM Credit Trading" 
                                    :subbusiness "CEEMEA Credit Trading"})
    (sql/insert! db-con :hierarchy {:strategy "001" 
                                    :business "Flow Credit Trading" 
                                    :subbusiness "EMEA Flow Credit Trading"})

    ;; Risk
    (sql/execute! db-con ["drop table risk if exists"])
    (sql/execute! db-con ["create table risk (tradeid int, pv double, cr01 double, ir01 double, theta double)"])
    (sql/insert! db-con :risk {:tradeid 1 :pv 1200 :cr01 3 :ir01 1.5 :theta 0.2})
    (sql/insert! db-con :risk {:tradeid 2 :pv -2000 :cr01 -4 :ir01 -1 :theta 0.3})
    
    ))

(defn query [sql]
  (sql/with-db-connection [db-con db-spec]
    (sql/query db-con [sql])))

#_(query "select a.* from accounts a join hierarchy h where h.strategy = a.strategy and h.business = 'EM Credit Trading'")

(use-fixtures :once 
  (fn [f] 
    (bootstrap-db)
    (f)))

(deftest test-sql-query-conversion
  (let [sut (dataset.core.ApplicativeQueryable. 
             (dataset.core.SQLQueryTransformer. #{"concat"}))]
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


    ;; functions
    (is (= "concat('hello','world')" (-parse-sexp sut '(concat "hello" "world"))))
    (is (= "concat(greeting,'world')" (-parse-sexp sut '(concat :$greeting "world"))))

    ;; garbage
    (is (= invalid (-parse-sexp sut '(garf "hello" "world"))))
    
    ;; combinations
    (is (= "(greeting = concat('hello','world'))"
           (-parse-sexp sut '(= :$greeting (concat "hello" "world")))))
    ))


(deftest test-sql-datasource
  (let [accounts (sql-table->dataset h2-spec "accounts")
        hierarchy (sql-table->dataset h2-spec "hierarchy")
        accsov (sql-query->dataset h2-spec "select * from accounts where mnemonic = 'ACCSOV'")]
    
    (is (= 3 (r/count accounts)))

    (is (= #{{:mnemonic "ACCSOV"} {:mnemonic "ACCRUS"} {:mnemonic "ACCAFR"}}
           (r/into #{} (select accounts :$mnemonic))))
    
    (is (= #{"ACCSOV" "ACCRUS" "ACCAFR"}
           (r/into #{} (r/map :mnem (select accounts [:$mnemonic :as :mnem])))))

    (is (= [{:mnemonic "ACCSOV" :strategy "001" :strategydescription "Sovereigns"}]
           (r/into [] accsov)))

    (is (= [{:mnem "ACCSOV"}]
           (r/into [] (select accsov [:$mnemonic :as :mnem]))))


    ;; Non-SQL interactions
    (is (= #{"SOV" "RUS" "AFR"}
           (r/into #{} (r/map :mnem (select accounts [(subs :$mnemonic 3) :as :mnem])))))

    ))

(deftest test-clojure-datasource
  (let [data (->clojure-dataset [{:mnemonic "ACCSOV"}
                                 {:mnemonic "ACCRUS"}
                                 {:mnemonic "ACCAFR"}])]
    (is (= #{{:mnem "SOV"} {:mnem "RUS"} {:mnem "AFR"}}
           (r/into #{} (select data [(subs :$mnemonic 3) :as :mnem]))))

    (is (= ["ACCSOV"]
           (r/into [] (r/map :mnemonic (where data (.contains :$mnemonic "SOV")))))))
  )
