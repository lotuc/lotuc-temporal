(ns lotuc.sci-rt.temporal.workflow.promise
  (:require
   [clojure.java.data :as j]
   [lotuc.temporal.to-java])
  (:import
   [io.temporal.workflow Promise]))

(set! *warn-on-reflection* true)

(defn promise-completed? [^Promise v] (.isCompleted v))

(defn promise-get-failure
  ([^Promise v] (.getFailure v)))

(defn promise-get
  ([^Promise v] (.get v))
  ([^Promise v duration]
   (let [^java.time.Duration d (j/to-java java.time.Duration duration)]
     (.get v (.toMillis d) java.util.concurrent.TimeUnit/MILLISECONDS))))

(defn promise-get-cancellable
  ([^Promise v] (.cancellableGet v))
  ([^Promise v duration]
   (let [^java.time.Duration d (j/to-java java.time.Duration duration)]
     (.cancellableGet v (.toMillis d) java.util.concurrent.TimeUnit/MILLISECONDS))))

(defn promise-then
  ([^Promise v f] (promise-then v :handle f))
  ([^Promise v typ f]
   (case typ
     :handle
     (.handle v (reify io.temporal.workflow.Functions$Func2
                  (apply [_ v e] (f v e))))
     :apply
     (.thenApply v (reify io.temporal.workflow.Functions$Func1
                     (apply [_ v] (f v))))
     :compose                           ; same as :apply, except that `f` should
                                        ; return a new `Promise`.
     (.thenCompose v (reify io.temporal.workflow.Functions$Func1
                       (apply [_ v] (f v))))
     :handle-exception
     (.exceptionally v (reify io.temporal.workflow.Functions$Func1
                         (apply [_ v] (f v)))))))

(defn promise-all-of [promises]
  (Promise/allOf ^Iterable promises))

(defn promise-any-of [promises]
  (Promise/anyOf ^Iterable promises))
