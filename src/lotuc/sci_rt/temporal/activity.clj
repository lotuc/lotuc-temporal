(ns lotuc.sci-rt.temporal.activity
  (:require
   [lotuc.sci-rt.temporal.sci :as temporal.sci]
   [sci.core :as sci]))

(defmacro var-syms []
  `['~'input
    '~'params])

(defn new-sci-vars []
  (into {} (for [k (var-syms)]
             [k (sci/new-dynamic-var k nil)])))

(def ^:dynamic input (sci/new-dynamic-var 'input nil))

(def ^:dynamic params (sci/new-dynamic-var 'params nil))

(def ^:dynamic sci-vars nil)

(defmacro with-shared-dynamic-vars-bound-to-sci-vars* [var-vals & body]
  `(temporal.sci/with-shared-dynamic-vars-bound-to-sci-vars
     ~(namespace `sci-vars) ~`sci-vars ~(var-syms) ~var-vals ~@body))

(defmacro with-shared-dynamic-vars-copied-out-sci* [& body]
  `(temporal.sci/with-shared-dynamic-vars-copied-out-sci
     ~(namespace `sci-vars) ~`sci-vars ~(var-syms) ~@body))

(defn wrap-fn-with-shared-dynamic-vars-copied-out-sci [f]
  (fn [& args]
    (with-shared-dynamic-vars-copied-out-sci* (apply f args))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn execution-ctx
  ^io.temporal.activity.ActivityExecutionContext []
  (io.temporal.activity.Activity/getExecutionContext))
