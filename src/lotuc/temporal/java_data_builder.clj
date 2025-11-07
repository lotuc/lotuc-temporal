(ns lotuc.temporal.java-data-builder
  (:require [clojure.java.data :as j]
            [clojure.java.data.builder :as j.builder]))

(set! *warn-on-reflection* true)

(defn- get-builder ^java.lang.reflect.Method [^Class clazz methods opts]
  (let [build-name (:build-fn opts)
        valid-builder-return-type?
        (if-some [pred (:valid-builder-return-type? opts)]
          (fn [t] (or (= clazz t) (pred t)))
          (fn [t] (= clazz t)))
        candidates
        (filter (fn [^java.lang.reflect.Method m]
                  (and (= 0 (alength ^"[Ljava.lang.Class;" (.getParameterTypes m)))
                       (valid-builder-return-type? (.getReturnType m))
                       (or (nil? build-name)
                           (= build-name (.getName m)))))
                methods)]
    (case (count candidates)
      0 (throw (IllegalArgumentException.
                (str "Cannot find builder method that returns "
                     (.getName clazz))))
      1 (first candidates)
      (let [builds (filter (fn [^java.lang.reflect.Method m]
                             (= "build" (.getName m)))
                           candidates)]
        (case (count builds)
          0 (throw (IllegalArgumentException.
                    (str "Cannot find 'build' method that returns "
                         (.getName clazz))))
          (first builds))))))

(defn ^{:author "Sean Corfield, modified by Lotuc, added `valid-setter-return-type?`."}
  find-setters [^Class builder methods props opts]
  ;;  Copyright (c) Sean Corfield. All rights reserved.
  ;;  The use and distribution terms for this software are covered by the
  ;;  Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
  ;;  which can be found in the file epl-v10.html at the root of this
  ;;  distribution.  By using this software in any fashion, you are agreeing to
  ;;  be bound by the terms of this license.  You must not remove this notice,
  ;;  or any other, from this software.
  ;;
  ;; https://github.com/clojure/java.data/blob/master/src/main/clojure/clojure/java/data/builder.clj
  (let [valid-setter-return-type?
        (if-some [pred (:valid-setter-return-type? opts)]
          (fn [t] (or (= builder t) (pred t)))
          (fn [t] (= builder t)))
        candidates
        (filter (fn [^java.lang.reflect.Method m]
                  (and (= 1 (alength ^"[Ljava.lang.Class;" (.getParameterTypes m)))
                       (valid-setter-return-type? (.getReturnType m))
                       (or (not (re-find #"^set[A-Z]" (.getName m)))
                           (not (:ignore-setters? opts)))))
                methods)]
    (->> candidates
         (reduce
          (fn [setter-map ^java.lang.reflect.Method m]
            (let [prop (keyword
                        (cond (re-find #"^set[A-Z]" (.getName m))
                              (let [^String n (subs (.getName m) 3)]
                                (str (Character/toLowerCase (.charAt n 0)) (subs n 1)))
                              (re-find #"^with[A-Z]" (.getName m))
                              (let [^String n (subs (.getName m) 4)]
                                (str (Character/toLowerCase (.charAt n 0)) (subs n 1)))
                              :else
                              (.getName m)))]
              (if (contains? props prop)
                (if (contains? setter-map prop)
                  (let [clazz1 (#'j/get-setter-type (second (get setter-map prop)))
                        clazz2 (#'j/get-setter-type m)
                        p-val  (get props prop)]
                    (cond (and (instance? clazz1 p-val)
                               (not (instance? clazz2 p-val)))
                          setter-map    ; existing setter is a better match:
                          (and (not (instance? clazz1 p-val))
                               (instance? clazz2 p-val))
                          ;; this setter is a better match:
                          (assoc setter-map prop [(#'j/make-setter-fn m) m])
                          :else         ; neither is an obviously better match:
                          (throw (IllegalArgumentException.
                                  (str "Duplicate setter found for " prop
                                       " in " (.getName builder) " class")))))
                  (assoc setter-map prop [(#'j/make-setter-fn m) m]))
                ;; if we are not trying to set this property, ignore the setter:
                setter-map)))
          {})
         (reduce-kv
          (fn [m k v]
            (assoc m k (first v)))
          {}))))

(defmacro with-patch [& body]
  `(with-redefs [~`j.builder/find-setters ~`find-setters
                 ~`j.builder/get-builder ~`get-builder]
     ~@body))

(defn to-java [^Class clazz ^Class builder instance props opts]
  (with-patch
    (j.builder/to-java clazz builder instance props opts)))
