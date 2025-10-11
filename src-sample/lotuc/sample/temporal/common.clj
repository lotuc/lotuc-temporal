(ns lotuc.sample.temporal.common
  (:require
   [integrant.core :as ig]
   [integrant.repl.state :as repl.state]))

(defn ^{:doc "Get current workflow in the context of workflow run.

  https://javadoc.io/doc/io.temporal/temporal-sdk/latest/io/temporal/workflow/WorkflowInfo.html"}
  workflow-info []
  (io.temporal.workflow.Workflow/getInfo))

(defn ^{:doc " Get current activity execution context in the context of activity run.

  https://javadoc.io/doc/io.temporal/temporal-sdk/latest/io/temporal/activity/ActivityExecutionContext.html"}
  activity-execution-ctx []
  (io.temporal.activity.Activity/getExecutionContext))

(defn activity-log
  [^io.temporal.activity.ActivityExecutionContext ctx & args]
  (locking *out*
    (let [info (.getInfo ctx)]
      (apply println
             (format "\n[wf<%s> %s/%s]\n[acti<%s> %s/%s <- %s]\n    "
                     (.getWorkflowType info) (.getWorkflowNamespace info) (.getWorkflowId info)
                     (.getActivityType info) (.getActivityNamespace info) (.getActivityId info)
                     (.getActivityTaskQueue info))
             args)
      (flush))))

(defn workflow-log
  [^io.temporal.workflow.WorkflowInfo workflow-info & args]
  (locking *out*
    (apply println
           (format "\n[wf<%s> %s/%s <- %s]\n    "
                   (.getWorkflowType workflow-info) (.getNamespace workflow-info) (.getWorkflowId workflow-info)
                   (.getTaskQueue workflow-info))
           args)
    (flush)))

(defn start-worker!
  [{:keys [client task-queue workflow-classes activity-instances
           ^io.temporal.worker.WorkerOptions worker-options]}]
  (let [worker-factory (io.temporal.worker.WorkerFactory/newInstance client)
        worker (if worker-options
                 (.newWorker worker-factory task-queue worker-options)
                 (.newWorker worker-factory task-queue))]
    (when (seq workflow-classes)
      (->> (into-array Class workflow-classes)
           (.registerWorkflowImplementationTypes worker)))
    (when (seq activity-instances)
      (->> (into-array Object activity-instances)
           (.registerActivitiesImplementations worker)))
    (.start worker-factory)
    {:client client :worker worker :worker-factory worker-factory}))

;; io.temporal.worker.WorkerFactory

(defn stop-worker! [{:keys [worker-factory]}]
  (when worker-factory
    (.shutdown worker-factory)
    (while (not (.isTerminated worker-factory))
      (println "wait for termination")
      (.awaitTermination worker-factory 1 java.util.concurrent.TimeUnit/SECONDS))))

(def +default-service-stubs+
  (delay (io.temporal.serviceclient.WorkflowServiceStubs/newLocalServiceStubs)))

(def +default-client+
  (delay (io.temporal.client.WorkflowClient/newInstance @+default-service-stubs+)))

(def +default-schedule-client+
  (delay (io.temporal.client.schedules.ScheduleClient/newInstance @+default-service-stubs+)))

(defmethod ig/init-key :sample/default-service-stubs [_ _] @+default-service-stubs+)
(defmethod ig/init-key :sample/default-client [_ _] @+default-client+)
(defmethod ig/init-key :sample/default-schedule-client [_ _] @+default-schedule-client+)

(defmethod ig/init-key :sample/worker [_ opts] (start-worker! opts))
(defmethod ig/halt-key! :sample/worker [_ component] (stop-worker! component))

(defn default-service-stubs [] (:sample/default-service-stubs repl.state/system))
(defn default-client [] (:sample/default-client repl.state/system))
(defn default-schedule-client [] (:sample/default-schedule-client repl.state/system))
