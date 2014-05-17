(ns dataset.query
  (:require [clojure.string :as s])
  (:use [dataset.core :only [field-ref? field->field-name]]))

(def invalid ::invalid)

(defprotocol SexpQueryTransformer
  (-to-value [self primitive])
  (-to-field [self field-name])
  (-operator? [self opname])
  (-function? [self fname]))

(defprotocol Queryable
  (-parse-sexp [self sexp]))


;; nested complex queries as well as simple statements:
;; - (= (+ :$a :$b) 2)
;; - (< (+ :$a 1) 5)
(deftype ApplicativeQueryable [transformer]
  Queryable
  (-parse-sexp [self sexp]
    (cond
      (list? sexp)
      (let [args (map #(-parse-sexp self %) (rest sexp))
            invalid? (some #{invalid} args)]
        (cond
          invalid? invalid
          (-operator? transformer (str (first sexp))) (str "(" (s/join (str " " (first sexp) " ") args) ")")
          (-function? transformer (str (first sexp))) (str (str (first sexp)) "(" (s/join "," args) ")")
          :else invalid))

      (or (set? sexp) (sequential? sexp))
      (str "(" (s/join "," (map #(-parse-sexp self %) sexp)) ")")

      (field-ref? sexp)
      (-to-field transformer (field->field-name sexp))

      :else (-to-value transformer sexp))))

;; Queryable that evaluates query directly, no field references allowed
(deftype ClojureQueryable []
  Queryable
  (-parse-sexp [self sexp]
    (cond
      (list? sexp)
      (let [args (map #(-parse-sexp self %) (rest sexp))]
        (if (some #{invalid} args) invalid (eval (apply list (first sexp) args))))

      (set? sexp)
      (let [vals (map #(-parse-sexp self %) sexp)]
        (if (some #{invalid} vals) invalid (set vals)))

      (sequential? sexp)
      (let [vals (map #(-parse-sexp self %) sexp)]
        (if (some #{invalid} vals) invalid vals))

      (field-ref? sexp) invalid
      :else sexp)))

;; single level queryable, nested queries are evaluated in Clojure where possible (i.e. known functions and no field references)
;; - (= :$a 1)
;; - (in :$a #{1 2 3})
;; - (= :$field (+ 1 2 3))
(deftype SimpleQueryable [supported-field? supported-ops]
  Queryable
  (-parse-sexp [self sexp]
    (if (list? sexp)
      (let [args (map
                   (fn [v]
                     (if (field-ref? v)
                       (if (supported-field? v) v invalid)
                       (-parse-sexp (ClojureQueryable.) v)))
                   (rest sexp))]
        (if (or (some #{invalid} args) (not (contains? supported-ops (first sexp))))
          invalid
          (apply list (first sexp) args)))
      invalid)))