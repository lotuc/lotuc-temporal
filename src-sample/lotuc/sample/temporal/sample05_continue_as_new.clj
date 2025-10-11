(ns lotuc.sample.temporal.sample05-continue-as-new
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.common :as common]
   [lotuc.sample.temporal.sample-activities :as sample-activities]
   [lotuc.sample.temporal.sample-workflows :as sample-workflows]))

(definterface ^{io.temporal.workflow.WorkflowInterface []} WfHelloAllTheTime
  (^{io.temporal.workflow.WorkflowMethod {:name "hello-all-the-time"}} hello [_params]))

(defrecord WfHelloNTimesImpl []
  WfHelloAllTheTime
  (hello [_ {:strs [n max-n message]}]
    (let [n (or n 0)
          max-n (or max-n 1000)]
      (try
        (doseq [i (range n max-n)]
          (let [activities
                (io.temporal.workflow.Workflow/newActivityStub
                 sample-activities/hello-activity
                 (-> (io.temporal.activity.ActivityOptions/newBuilder)
                     (.setStartToCloseTimeout (java.time.Duration/ofSeconds 10))
                     (.setHeartbeatTimeout (java.time.Duration/ofSeconds 1))
                     (.build)))]
            (.hello activities (format "[%s] passed from wf: %s" i message)))
          ;; continue as new when current workflow ran for 10 times.
          ;;
          ;; passing state to the next run, for this workflow, it means the next
          ;; `n` to be run.
          (when (> (- i n) 10)
            (io.temporal.workflow.Workflow/continueAsNew
             (into-array Object [{"n" (inc i) "max-n" max-n "message" message}]))
            (throw (ex-info "continue as new" {::continue-as-new true}))))
        (catch Throwable t
          (when-not (::continue-as-new (ex-data t))
            (throw t)))))))

(def task-queue (str ::queue))

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client {}
    :sample/default-schedule-client {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [sample-workflows/wf-hello-impl WfHelloNTimesImpl]
     :activity-instances [(sample-activities/->HelloActivityImpl)]}}))

(defn create-&-run-workflow [workflow-id msg]
  (let [options
        (-> (io.temporal.client.WorkflowOptions/newBuilder)
            (.setWorkflowId workflow-id)
            (.setTaskQueue task-queue)
            (.build))]
    (future
      (-> (.newWorkflowStub (common/default-client) WfHelloAllTheTime options)
          (.hello msg)))))

(comment
  (create-&-run-workflow "wf002" {"max-n" 30 "message" "hello lotuc"})
  #_())
