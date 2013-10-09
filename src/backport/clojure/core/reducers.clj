;   Copyright (c) Rich Hickey. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns ^{:doc
      "A library for reduction and parallel folding. Alpha and subject
      to change."
      :author "Rich Hickey"}
  backport.clojure.core.reducers
  (:refer-clojure :exclude [into reduce map mapcat filter remove take take-while 
                            drop flatten partition partition-all group-by count])
  (:require [clojure.walk :as walk]
            backport.clojure.core.protocols))

(alias 'core 'clojure.core)
(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn reduce
  "Like core/reduce except:
     When init is not provided, (f) is used.
     Maps are reduced with reduce-kv"
  ([f coll] (reduce f (f) coll))
  ([f init coll]
     (if (instance? java.util.Map coll)
       (backport.clojure.core.protocols/kv-reduce coll f init)
       (backport.clojure.core.protocols/coll-reduce coll f init))))

(defn into
  "Returns a new coll consisting of to-coll with all of the items of
  from-coll conjoined."
  {:added "1.0"
   :static true}
  [to from]
  (if (instance? clojure.lang.IEditableCollection to)
    (with-meta (persistent! (reduce conj! (transient to) from)) (meta to))
    (reduce conj to from)))

(defn reducer
  "Given a reducible collection, and a transformation function xf,
  returns a reducible collection, where any supplied reducing
  fn will be transformed by xf. xf is a function of reducing fn to
  reducing fn."
  {:added "1.5"}
  ([coll xf]
     (reify
      backport.clojure.core.protocols/CollReduce
      (coll-reduce [this f1]
                   (backport.clojure.core.protocols/coll-reduce this f1 (f1)))
      (coll-reduce [_ f1 init]
                   (backport.clojure.core.protocols/coll-reduce coll (xf f1) init)))))

(defn folder
  "Given a foldable collection, and a transformation function xf,
  returns a foldable collection, where any supplied reducing
  fn will be transformed by xf. xf is a function of reducing fn to
  reducing fn."
  {:added "1.5"}
  ([coll xf]
     (reify
      backport.clojure.core.protocols/CollReduce
      (coll-reduce [_ f1]
                   (backport.clojure.core.protocols/coll-reduce coll (xf f1) (f1)))
      (coll-reduce [_ f1 init]
                   (backport.clojure.core.protocols/coll-reduce coll (xf f1) init)))))

(defn- do-curried
  [name doc meta args body]
  (let [cargs (vec (butlast args))]
    `(defn ~name ~doc ~meta
       (~cargs (fn [x#] (~name ~@cargs x#)))
       (~args ~@body))))

(defmacro ^:private defcurried
  "Builds another arity of the fn that returns a fn awaiting the last
  param"
  [name doc meta args & body]
  (do-curried name doc meta args body))

(defn- do-rfn [f1 k fkv]
  `(fn
     ([] (~f1))
     ~(clojure.walk/postwalk
       #(if (sequential? %)
          ((if (vector? %) vec identity)
           (core/remove #{k} %))
          %)
       fkv)
     ~fkv))

(defmacro ^:private rfn
  "Builds 3-arity reducing fn given names of wrapped fn and key, and k/v impl."
  [[f1 k] fkv]
  (do-rfn f1 k fkv))

(defcurried map
  "Applies f to every value in the reduction of coll. Foldable."
  {:added "1.5"}
  [f coll]
  (folder coll
   (fn [f1]
     (rfn [f1 k]
          ([ret k v]
             (f1 ret (f k v)))))))

(defcurried mapcat
  "Applies f to every value in the reduction of coll, concatenating the result
  colls of (f val). Foldable."
  {:added "1.5"}
  [f coll]
  (folder coll
   (fn [f1]
     (rfn [f1 k]
          ([ret k v]
             (reduce f1 ret (f k v)))))))

(defn count [coll]
  (if (instance? clojure.lang.Counted coll)
    (clojure.core/count coll)
    (reduce + 0 (map (constantly 1) coll))))

(defcurried filter
  "Retains values in the reduction of coll for which (pred val)
  returns logical true. Foldable."
  {:added "1.5"}
  [pred coll]
  (folder coll
   (fn [f1]
     (rfn [f1 k]
          ([ret k v]
             (if (pred k v)
               (f1 ret k v)
               ret))))))

(defcurried remove
  "Removes values in the reduction of coll for which (pred val)
  returns logical true. Foldable."
  {:added "1.5"}
  [pred coll]
  (filter (complement pred) coll))

(defcurried flatten
  "Takes any nested combination of sequential things (lists, vectors,
  etc.) and returns their contents as a single, flat foldable
  collection."
  {:added "1.5"}
  [coll]
  (folder coll
   (fn [f1]
     (fn
       ([] (f1))
       ([ret v]
          (if (sequential? v)
            (backport.clojure.core.protocols/coll-reduce (flatten v) f1 ret)
            (f1 ret v)))))))

(defcurried take-while
  "Ends the reduction of coll when (pred val) returns logical false."
  {:added "1.5"}
  [pred coll]
  (reducer coll
   (fn [f1]
     (rfn [f1 k]
          ([ret k v]
             (if (pred k v)
               (f1 ret k v)
               (backport.clojure.core.protocols/reduced ret)))))))

(defcurried take
  "Ends the reduction of coll after consuming n values."
  {:added "1.5"}
  [n coll]
  (reducer coll
   (fn [f1]
     (let [cnt (atom n)]
       (rfn [f1 k]
         ([ret k v]
            (swap! cnt dec)
            (if (neg? @cnt)
              (backport.clojure.core.protocols/reduced ret)
              (f1 ret k v))))))))

(defcurried drop
  "Elides the first n values from the reduction of coll."
  {:added "1.5"}
  [n coll]
  (reducer coll
   (fn [f1]
     (let [cnt (atom n)]
       (rfn [f1 k]
         ([ret k v]
            (swap! cnt dec)
            (if (neg? @cnt)
              (f1 ret k v)
              ret)))))))

(defn partition
  "Partitions sequence into sliding windows of size N moving forward by step"
  ([n coll] (partition n n coll))
  ([n step coll]
     (reducer coll
              (fn [f1]
                (let [accu (atom [])]
                  (fn 
                    ([] (f1))
                    ([ret v]
                       (swap! accu clojure.core/conj v)
                       (if (= (count @accu) n)
                         (let [ret# (f1 ret @accu)]
                           (swap! accu subvec step)
                           ret#)
                         ret))))))))

(defn uncounted-cat [left right]
  (reify
    clojure.lang.Seqable
    (seq [_] (concat (seq left) (seq right)))

    backport.clojure.core.protocols/CollReduce
    (coll-reduce [this f1] (backport.clojure.core.protocols/coll-reduce this f1 (f1)))
    (coll-reduce
      [_  f1 init]
      (backport.clojure.core.protocols/coll-reduce
       right f1
       (backport.clojure.core.protocols/coll-reduce left f1 init)))))

(defn partition-all
  ([n coll] (partition-all n n coll))
  ([n step coll]
     (let [o (Object.)]
       (reducer (uncounted-cat coll (repeat o))
                (fn [f1]
                  (let [accu (atom [])]
                    (fn 
                      ([] (f1))
                      ([ret v]
                         (swap! accu clojure.core/conj v)
                         (cond 
                          (= (last @accu) o)
                          (let [sanitized (clojure.core/remove #{o} @accu)]
                            (backport.clojure.core.protocols/reduced 
                             (if (empty? sanitized)
                               ret
                               (f1 ret sanitized))))

                          (= (count @accu) n)
                          (let [ret# (f1 ret @accu)]
                            (swap! accu subvec step)
                            ret#)

                          :else ret)))))))))




(defn group-by 
  "Returns a map of the elements of coll keyed by the result of
  f on each element. The value at each key will be a vector of the
  corresponding elements, in the order they appeared in coll."
  [f coll]  
  (persistent!
   (reduce
    (fn [ret x]
      (let [k (f x)]
        (assoc! ret k (conj (get ret k []) x))))
    (transient {}) coll)))




;;do not construct this directly, use cat
(deftype Cat [cnt left right]
  clojure.lang.Counted
  (count [_] cnt)

  clojure.lang.Seqable
  (seq [_] (concat (seq left) (seq right)))

  backport.clojure.core.protocols/CollReduce
  (coll-reduce [this f1] (backport.clojure.core.protocols/coll-reduce this f1 (f1)))
  (coll-reduce
   [_  f1 init]
   (backport.clojure.core.protocols/coll-reduce
    right f1
    (backport.clojure.core.protocols/coll-reduce left f1 init))))

(defn cat
  "A high-performance combining fn that yields the catenation of the
  reduced values. The result is reducible, foldable, seqable and
  counted, providing the identity collections are reducible, seqable
  and counted. The single argument version will build a combining fn
  with the supplied identity constructor. Tests for identity
  with (zero? (count x)). See also foldcat."
  {:added "1.5"}
  ([] (java.util.ArrayList.))
  ([ctor]
     (fn
       ([] (ctor))
       ([left right] (cat left right))))
  ([left right]
     (cond
      (zero? (count left)) right
      (zero? (count right)) left
      :else
      (Cat. (+ (count left) (count right)) left right))))

(defn append!
  ".adds x to acc and returns acc"
  {:added "1.5"}
  [^java.util.Collection acc x]
  (doto acc (.add x)))

(defn monoid
  "Builds a combining fn out of the supplied operator and identity
  constructor. op must be associative and ctor called with no args
  must return an identity value for it."
  {:added "1.5"}
  [op ctor]
  (fn m
    ([] (ctor))
    ([a b] (op a b))))
