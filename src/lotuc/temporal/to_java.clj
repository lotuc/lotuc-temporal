(ns lotuc.temporal.to-java
  (:require
   [clojure.java.data :as j]
   [clojure.java.data.builder :as j.builder]))

(defmacro new-persistent-map-to-java [& class-list]
  `(do
     ~@(for [clazz class-list]
         `(clojure.core/defmethod ~`j/to-java [~clazz clojure.lang.PersistentArrayMap]
            [_# v#]
            (~`j.builder/to-java
             ~clazz
             ~(symbol (str clazz "$Builder"))
             (~(symbol (str clazz "/newBuilder")))
             v#
             {:build-fn "build"})))))

(defmethod j/to-java [java.time.Duration clojure.lang.PersistentVector]
  [_ [t n]]
  (case (keyword t)
    :ms (java.time.Duration/ofMillis n)
    :sec (java.time.Duration/ofSeconds n)
    :min (java.time.Duration/ofMinutes n)
    :hour (java.time.Duration/ofHours n)
    :day (java.time.Duration/ofDays n)))

(new-persistent-map-to-java
 io.temporal.activity.ActivityOptions

 io.temporal.client.WorkflowOptions
 io.temporal.client.WorkflowClientOptions

 io.temporal.client.schedules.Schedule
 io.temporal.client.schedules.ScheduleActionStartWorkflow
 io.temporal.client.schedules.ScheduleSpec
 io.temporal.client.schedules.ScheduleOptions

 io.temporal.common.RetryOptions
 io.temporal.common.Priority

 io.temporal.worker.WorkerOptions

 io.temporal.workflow.ChildWorkflowOptions
 io.temporal.workflow.TimerOptions)
