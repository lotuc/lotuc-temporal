(ns lotuc.sample.temporal.sample04-schedule
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.common :as common]
   [lotuc.sample.temporal.sample-activities :as sample-activities]
   [lotuc.sample.temporal.sample-workflows :as sample-workflows]))

;;; - https://docs.temporal.io/schedule
;;; - https://docs.temporal.io/develop/java/schedules

(def schedule-id "sched42")
(def task-queue (str ::queue))

(ig.repl/set-prep!
 #(ig/expand {:sample/default-client {}
              :sample/default-schedule-client {}
              :sample/worker
              {:client (ig/ref :sample/default-client)
               :task-queue task-queue
               :workflow-classes [sample-workflows/wf-hello-impl]
               :activity-instances [(sample-activities/->HelloActivityImpl)]}}))

(def schedule
  (-> (io.temporal.client.schedules.Schedule/newBuilder)
      (.setAction
       (-> (io.temporal.client.schedules.ScheduleActionStartWorkflow/newBuilder)
           (.setWorkflowType sample-workflows/wf-hello)
           (.setArguments (into-array Object ["hello from schedule"]))
           (.setOptions
            (-> (io.temporal.client.WorkflowOptions/newBuilder)
                (.setWorkflowId "wfhello-from-schedule-0")
                (.setTaskQueue task-queue)
                (.build)))
           (.build)))
      (.setSpec
       (-> (io.temporal.client.schedules.ScheduleSpec/newBuilder)
           (.build)))
      (.build)))

(comment

;;; create & get schedule handle
  (def handle
    (-> (common/default-schedule-client)
        (.createSchedule
         schedule-id schedule
         (-> (io.temporal.client.schedules.ScheduleOptions/newBuilder)
             (.build)))))

;;; get handle for created schedule
  (def handle
    (.getHandle (common/default-schedule-client) schedule-id))

;;; delete schedule
  (.delete handle)

;;; triggers an immediate action with a given Schedule
  (.trigger handle)

;;; update schedule
  (->> (fn [input]
         (-> (io.temporal.client.schedules.Schedule/newBuilder
              (.. input (getDescription) (getSchedule)))
             (.setState
              ;; run 8 times
              (-> (io.temporal.client.schedules.ScheduleState/newBuilder)
                  (.setLimitedAction true)
                  (.setRemainingActions 8)
                  (.build)))
             (.setSpec
              ;; run schedule action every 10s
              (-> (io.temporal.client.schedules.ScheduleSpec/newBuilder)
                  (.setIntervals [(io.temporal.client.schedules.ScheduleIntervalSpec.
                                   (java.time.Duration/ofSeconds 10))])
                  (.build)))
             (.build)
             (io.temporal.client.schedules.ScheduleUpdate.)))
       (.update handle))

;;; pause schedule
  (.pause handle)
  (.unpause handle)

;;; list schedules
  (->> (.listSchedules (common/default-schedule-client))
       (.iterator) (iterator-seq)
       (map bean))

  ;; describe
  (-> (.getHandle (common/default-schedule-client) schedule-id)
      (.describe) (bean) (:info) (bean))

  (-> (.getHandle (common/default-schedule-client) "some-random-id")
      (.describe))

;;; Backfill

;;; Actions within given timeperiods might not be ran because of pausing (or
;;; some other reasons). Trigger a backfill would cause those actions to be ran.

  (let [now (java.time.Instant/now)]
    (.backfill handle
               [(io.temporal.client.schedules.ScheduleBackfill.
                 (.minusMillis now 5500)
                 (.minusMillis now 2500))
                (io.temporal.client.schedules.ScheduleBackfill.
                 (.minusMillis now 2500)
                 now)]))

  #_())
