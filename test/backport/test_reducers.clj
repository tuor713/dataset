(ns backport.test-reducers
  "A couple of tests to verify the primitives work as expected with the backported reducers (in particular implementation of reduced / reduced? and interactions with clojure.core data structures like chunked sequences.
Also test various clojure.core functions ported to reducers namespace (reduce, into, group-by)"
  (:use clojure.test)
  (:require [backport.clojure.core.reducers :as r]))


(deftest test-map
  (is (= [1 2 3 4 5] (r/into [] (r/map inc (range 5))))))

(deftest test-filter
  (is (= [0 2 4] (r/into [] (r/filter even? (range 5))))))

(deftest test-take
  (is (= [0 1 2] (r/into [] (r/take 3 (range)))))
  (is (= [] (r/into [] (r/take 0 (range))))))

(deftest test-take-while
  (is (= [0 1 2 3 4] (r/into [] (r/take-while #(< % 5) (range))))))

(deftest test-chaining
  (is (= [1 3 5] 
         (->> (range)
              (r/filter even?)
              (r/map inc)
              (r/take 3)
              (r/into [])))))

(deftest test-drop
  (is (= [2 3 4] (r/into [] (r/drop 2 (range 5))))))


(deftest test-partition
  ;; standard case
  (is (= (partition 2 (range 4))
         (r/into [] (r/partition 2 (range 4)))))

  ;; superfluous items at the end get dropped
  (is (= (partition 2 (range 5))
         (r/into [] (r/partition 2 (range 5)))))

  ;; overlapping windows
  (is (= (partition 2 1 (range 5))
         (r/into [] (r/partition 2 1 (range 5))))))

(deftest test-partition-all
  ;; standard case
  (is (= (partition-all 2 (range 4))
         (r/into [] (r/partition-all 2 (range 4)))))
  
  ;; superfluous items at the end get shortened
  (is (= (partition-all 2 (range 5))
         (r/into [] (r/partition-all 2 (range 5)))))

  ;; overlapping windows
  (is (= (partition-all 2 1 (range 5))
         (r/into [] (r/partition-all 2 1 (range 5))))))


(deftest test-group-by
  (is (= (group-by even? (range 5))
         (r/group-by even? (range 5)))))

