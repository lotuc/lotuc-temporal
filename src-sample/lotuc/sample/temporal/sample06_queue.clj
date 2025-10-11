(ns lotuc.sample.temporal.sample06-queue
  (:require
   [integrant.core :as ig]
   [integrant.repl]
   [lotuc.sample.temporal.common :as common]
   [lotuc.sample.temporal.sample-activities :as sample-activities]
   [lotuc.sample.temporal.sample-workflows :as sample-workflows]))

(defn build-worker-options [worker-tuner]
  (let [n-workflow (or (:workflow worker-tuner) 1)
        n-activity (or (:activity worker-tuner) 1)
        n-local-activity (or (:local-activity worker-tuner) 1)
        n-nexus (or (:nexus worker-tuner) 1)]
    (-> (io.temporal.worker.WorkerOptions/newBuilder)
        (.setWorkerTuner
         (io.temporal.worker.tuning.CompositeTuner.
          ;; workflow
          (io.temporal.worker.tuning.FixedSizeSlotSupplier. n-workflow)
          ;; activity
          (io.temporal.worker.tuning.FixedSizeSlotSupplier. n-activity)
          ;; local activity
          (io.temporal.worker.tuning.FixedSizeSlotSupplier. n-local-activity)
          ;; nexus
          (io.temporal.worker.tuning.FixedSizeSlotSupplier. n-nexus)))
        (.build))))

(def task-queue (str ::queue))

(derive ::worker0 :sample/worker)
(derive ::worker1 :sample/worker)
(derive ::worker2 :sample/worker)

(integrant.repl/set-prep!
 #(ig/expand
   {:sample/default-client {}

    ::worker0
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [sample-workflows/wf-math-impl]
     :worker-options (build-worker-options
                      {:workflow 2})}

    ::worker1
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :activity-instances
     [(sample-activities/->SimpleMathActivitiesImpl)]
     :worker-options
     (build-worker-options
      {:activity 5})}

    ::worker2
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :activity-instances
     [(sample-activities/->SimpleMathActivitiesImpl)]
     :worker-options
     (build-worker-options
      {:activity 5})}}))

(defn run-wf [opts]
  (let [options (-> (io.temporal.client.WorkflowOptions/newBuilder)
                    (.setTaskQueue task-queue)
                    (.build))]
    (-> (common/default-client)
        (.newWorkflowStub sample-workflows/wf-math options)
        (.run opts))))

(comment

  (time (run-wf {"op" "delayAdd" "args" [1 2] "opts" {"delayMs" 1000}}))

  (do (def tasks
        (doall
         (for [_ (range 50)]
           (future (run-wf {"op" "delayAdd" "args" [1 2] "opts" {"delayMs" 1000}})))))
      (future (time (doseq [t tasks] @t))))

  (mapv deref tasks)

  #_())
