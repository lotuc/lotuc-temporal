(ns lotuc.sci-rt.temporal.activity
  (:require
   [clojure.java.data :as j]
   [lotuc.sci-rt.temporal.sci :as temporal.sci]
   [sci.core :as sci]))

(defmacro var-syms []
  `['~'input
    '~'params])

(defn new-sci-vars []
  (into {} (for [k (var-syms)]
             [k (sci/new-dynamic-var k nil)])))

(def ^:dynamic input nil)

(def ^:dynamic params nil)

(def ^:dynamic sci-vars nil)

(defmacro with-shared-dynamic-vars-bound-to-sci-vars* [var-vals & body]
  `(temporal.sci/with-shared-dynamic-vars-bound-to-sci-vars
     ~(namespace `sci-vars) ~`sci-vars ~(var-syms) ~var-vals ~@body))

(defmacro with-shared-dynamic-vars-copied-out-sci* [& body]
  `(temporal.sci/with-shared-dynamic-vars-copied-out-sci
     ~(namespace `sci-vars) ~`sci-vars ~(var-syms) ~@body))

(defn wrap-fn-with-shared-dynamic-vars-copied-out-sci [f]
  {:pre [sci-vars]}
  (let [sci-vars* sci-vars]
    (fn [& args]
      (binding [sci-vars sci-vars*]
        (with-shared-dynamic-vars-copied-out-sci* (apply f args))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn execution-ctx
  ^io.temporal.activity.ActivityExecutionContext []
  (io.temporal.activity.Activity/getExecutionContext))

(defn activity-info [^io.temporal.activity.ActivityExecutionContext ctx]
  (j/from-java (.getInfo ctx)))

(defn heartbeat [^io.temporal.activity.ActivityExecutionContext ctx details]
  (.heartbeat ctx details))

(defn get-heartbeat-details [^io.temporal.activity.ActivityExecutionContext ctx]
  (.getHeartbeatDetails ctx Object))

(defn get-last-heartbeat-details [^io.temporal.activity.ActivityExecutionContext ctx]
  (.getLastHeartbeatDetails ctx Object))

(defn get-task-token [^io.temporal.activity.ActivityExecutionContext ctx]
  (.getTaskToken ctx))

(defn do-not-complete-on-return [^io.temporal.activity.ActivityExecutionContext ctx]
  (.doNotCompleteOnReturn ctx))

(defn do-not-complete-on-return? [^io.temporal.activity.ActivityExecutionContext ctx]
  (.isDoNotCompleteOnReturn ctx))

(defn get-workflow-client
  ^io.temporal.client.WorkflowClient
  [^io.temporal.activity.ActivityExecutionContext ctx]
  (.getWorkflowClient ctx))

(defn sci-fns [sci-vars*]
  (binding [sci-vars sci-vars*]
    (->> {'execution-ctx execution-ctx
          'activity-info activity-info
          'heartbeat heartbeat
          'get-heartbeat-details get-heartbeat-details
          'get-last-heartbeat-details get-last-heartbeat-details
          'get-task-token get-task-token
          'do-not-complete-on-return do-not-complete-on-return
          'do-not-complete-on-return? do-not-complete-on-return?
          'get-workflow-client get-workflow-client}
         (reduce-kv
          (fn [m k v]
            (assoc m k (wrap-fn-with-shared-dynamic-vars-copied-out-sci v)))
          {}))))
