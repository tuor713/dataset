(ns dataset.core
  (:refer-clojure :exclude [reduce into map mapcat filter remove take take-while drop flatten])
  (:use [backport.clojure.core.reducers :only 
         [reduce into map mapcat filter remove take take-while drop cat flatten drop]]))

;; Notes reducers implementations outperform normal collections in standard tasks yielding finite datasets
;; However, of course they must always have the final, finite reduction step




