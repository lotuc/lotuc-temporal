(ns lotuc.sample.temporal.sample02-child-wf
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.common :as common]
   [lotuc.sample.temporal.sample-activities :as sample-activities]
   [lotuc.sample.temporal.sample-workflows :as sample-workflows]))

(definterface ^{io.temporal.workflow.WorkflowInterface []} WfHelloNTimes
  (^{io.temporal.workflow.WorkflowMethod {:name "hello-n-times"}} hello [_params]))

(defrecord WfHelloNTimesImpl []
  WfHelloNTimes
  (hello [_ {:strs [n]}]
    (let [child-tasks
          (for [i (range (or n 3))]
            (let [_
                  (-> (io.temporal.workflow.Workflow/newTimer
                       (java.time.Duration/ofSeconds 3)
                       (-> (io.temporal.workflow.TimerOptions/newBuilder)
                           (.setSummary "wait for next")
                           (.build)))
                      (.get))

                  child
                  (io.temporal.workflow.Workflow/newChildWorkflowStub
                   sample-workflows/WfHello
                   (-> (io.temporal.workflow.ChildWorkflowOptions/newBuilder)
                       (.setWorkflowId
                        (str (.getWorkflowId (io.temporal.workflow.Workflow/getInfo))
                             "-child-" i))
                       (.setParentClosePolicy
                        io.temporal.api.enums.v1.ParentClosePolicy/PARENT_CLOSE_POLICY_ABANDON)
                       (.build)))]
              [child
               (io.temporal.workflow.Async/function
                (reify io.temporal.workflow.Functions$Func1
                  (apply [_ v] (.hello child v)))
                (str "message " i))]))]
      (doseq [[child _r] (doall child-tasks)]
       ;; wait for execution to start (not waiting for the result)
        (-> (io.temporal.workflow.Workflow/getWorkflowExecution child)
            (.get))))))

(def ^{:doc "queue for testing"} task-queue (str ::queue))

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client  {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [sample-workflows/wf-hello-impl WfHelloNTimesImpl]
     :activity-instances [(sample-activities/->HelloActivityImpl)]}}))

(defn create-&-run-workflow [workflow-id msg]
  (let [options
        (-> (io.temporal.client.WorkflowOptions/newBuilder)
            (.setWorkflowId workflow-id)
            (.setWorkflowIdConflictPolicy
             io.temporal.api.enums.v1.WorkflowIdConflictPolicy/WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
            (.setWorkflowIdReusePolicy
             io.temporal.api.enums.v1.WorkflowIdReusePolicy/WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
            (.setTaskQueue task-queue)
            (.build))]
    (future
      (-> (common/default-client)
          (.newWorkflowStub WfHelloNTimes options)
          (.hello msg)))))

(comment
  (def a0 (create-&-run-workflow (str (random-uuid)) {"n" 3}))
  #_())
