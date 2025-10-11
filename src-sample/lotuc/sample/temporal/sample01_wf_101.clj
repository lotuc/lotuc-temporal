(ns lotuc.sample.temporal.sample01-wf-101
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.common :as common]
   [pronto.core :as pronto]))

;;; Notes on Java SDK

(definterface ^{io.temporal.activity.ActivityInterface []}
  HelloActivity
  (hello [v]))

;;; Activity method name:
;;;  0. ActivityMethod.name
;;;  1. ActivityInterface.namePrefix + Hello (Camel case)
;;;  2. Hello (Camel case)

(defrecord HelloActivityImpl []
  HelloActivity
  (hello [_ v] (prn :wf-hello-activity v) v))

(definterface ^{io.temporal.workflow.WorkflowInterface []}
  WfHello
  (^{io.temporal.workflow.WorkflowMethod []} hello [_params]))

;;; Workflow type name
;;;  0. io.temporal.workflow.WorkflowMethod.name
;;;  1. Class name

(defrecord WfHelloImpl []
  WfHello
  (hello [_ v]
    (let [activities
          (io.temporal.workflow.Workflow/newActivityStub
           HelloActivity
           (-> (io.temporal.activity.ActivityOptions/newBuilder)
               (.setStartToCloseTimeout (java.time.Duration/ofSeconds 10))
               (.build)))]
      (prn :wf-hello v)
      (.hello activities (str "passed from wf: " v)))
    v))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def task-queue (str ::queue))

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [WfHelloImpl]
     :activity-instances [(->HelloActivityImpl)]}}))

(defn wf-options [workflow-id]
  (-> (io.temporal.client.WorkflowOptions/newBuilder)
      (.setWorkflowId workflow-id)
      (.setTaskQueue task-queue)
      (.build)))

(defn create-&-run-workflow
  [workflow-id params]
  (future
    (-> (common/default-client)
        (.newWorkflowStub WfHello (wf-options workflow-id))
        (.hello params))))

(comment

  @(create-&-run-workflow "hello0" "world")

  #_())
