(ns dataset.core-test
  (:require [clojure.java.jdbc :as sql]
            [backport.clojure.core.reducers :as r])
  (:use clojure.test
        [clojure.java.io :only [file]]
        dataset.core
        dataset.csv
        dataset.query))


;;;;; Querable ;;;;;

(deftest test-simple-queryable
  (let [sut (dataset.query.SimpleQueryable. (constantly true) #{'= 'in})
        sut2 (dataset.query.SimpleQueryable. #{:$a :$b} #{'= 'in})]
    (is (= invalid (-parse-sexp sut '1)))
    (is (= '(= 1 2) (-parse-sexp sut '(= 1 2))))
    (is (= invalid (-parse-sexp sut '(< 1 2))))
    (is (= '(= :$a "hello") (-parse-sexp sut '(= :$a "hello"))))
    (is (= '(= :$a 42) (-parse-sexp sut '(= :$a (* 6 7)))))
    (is (= invalid (-parse-sexp sut '(= :$a (+ :$b 1)))))
    (is (= '(in :$a [1 2]) (-parse-sexp sut '(in :$a [1 2]))))

    (is (= invalid (-parse-sexp sut '(= :$a :$b)))
        "Multiple field references are rejected")

    (is (= '(= :$a 1) (-parse-sexp sut '(= 1 :$a)))
        "Equality order normalization")

    (testing "Restricted field list"
      (is (= '(= :$a 1) (-parse-sexp sut2 '(= :$a 1))))
      (is (= invalid (-parse-sexp sut2 '(= :$notsupported 1)))))

    ))



;;;;; Default clojure operations ;;;;;


(defn to-vec [ds] (r/into [] ds))
(defn to-set [ds] (r/into #{} ds))


(deftest test-clojure-datasource
  (let [data [{:mnemonic "ACCSOV"}
              {:mnemonic "ACCRUS"}
              {:mnemonic "ACCAFR"}]]
    (is (= #{{:mnem "SOV"} {:mnem "RUS"} {:mnem "AFR"}}
           (to-set (select data [(subs :$mnemonic 3) :as :$mnem]))))

    (is (= ["ACCSOV"]
           (to-vec (r/map :mnemonic (where data (.contains :$mnemonic "SOV"))))))))


(deftest test-clojure-joins
  (let [accounts [{:mnemonic "ACCSOV" :strategy "001" :strategydescription "Sovereigns"}
                  {:mnemonic "ACCRUS" :strategy "002" :strategydescription "Russia"}
                  {:mnemonic "ACCAFR" :strategy "002" :strategydescription "Africa"}]
        hierarchy [{:strategy "001"
                    :business "Flow Credit Trading"
                    :subbusiness "EMEA Flow Credit Trading"}
                   {:strategy "002"
                    :business "EM Credit Trading"
                    :subbusiness "CEEMEA Credit Trading"}
                   {:strategy "003"
                    :business "Structured Credit Trading"
                    :subbusiness "CDO Trading"}]]

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


(deftest test-csvs
  (is (= #{"elephant" "lion" "kangaroo" "bison"}
         (r/into #{} (r/map :name (csv-dataset (file "test/data/animals.csv"))))))
  (is (= #{"elephant" "lion" "kangaroo" "bison"}
         (r/into #{} (r/map :name (csv-dataset (file "test/data/animals2.csv") \|)))))
  (is (= {:continent "africa;asia", :type "herbivore", :name "elephant"}
         (first (seq (csv-dataset (file "test/data/animals.csv")))))))

