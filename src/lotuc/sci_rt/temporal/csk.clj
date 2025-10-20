(ns lotuc.sci-rt.temporal.csk
  (:require
   [camel-snake-kebab.core :as csk]
   [camel-snake-kebab.extras :as cske]
   [clojure.walk :as walk]))

;;; compile time check
(assert (= :md5 (csk/->kebab-case-keyword "md5")))

(defn ^{:doc "@see `clojure.walk/walk`"}
  walkable?
  [form]
  (or
   (list? form)
   (instance? clojure.lang.IMapEntry form)
   (seq? form)
   (instance? clojure.lang.IRecord form)
   (coll? form)))

(defn ->walkable [v]
  (letfn [(->one [v]
            (cond
              (walkable? v) v
              (instance? java.util.List v) (->all (into [] v))
              (instance? java.util.Map v) (->all (into {} v))
              (instance? java.util.Iterator v) (map ->all (iterator-seq v))
              :else v))
          (->all [v]
            (walk/postwalk ->one v))]
    (->all v)))

(defn ->string [v]
  (if (or (symbol? v) (keyword? v)) (name v) v))

(defn ->keyword [v]
  (if (or (symbol? v) (keyword? v) (string? v)) (keyword v) v))

(defn transform-keys [t v]
  (cske/transform-keys t (->walkable v)))

(comment
  (transform-keys csk/->kebab-case-string {:md5 "hello"}))

(defn wrap-fn
  ([f] (wrap-fn nil f))
  ([{:keys [t-params t-result]
     :or {t-params ->keyword
          t-result ->string}}
    f]
   (fn [& params]
     (->> (map #(transform-keys t-params %) params)
          (apply f)
          (transform-keys t-result)))))

(comment
  ((wrap-fn
    {:t-params ->string
     :t-result ->keyword}
    (fn f [opts] (prn opts) opts))
   {:hello "world"})

  ((wrap-fn
    {:t-params ->keyword
     :t-result ->string}
    (fn f [opts] (prn opts) opts))
   {"hello" "world"})
  #_())
