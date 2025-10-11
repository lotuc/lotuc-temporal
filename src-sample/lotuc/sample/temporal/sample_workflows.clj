(ns lotuc.sample.temporal.sample-workflows
  (:require
   [lotuc.sample.temporal.sample-activities :as sample-activities]))

(definterface ^{io.temporal.workflow.WorkflowInterface []} WfHello
  (^{io.temporal.workflow.WorkflowMethod {:name "hello"}} hello [_params]))

(defrecord WfHelloImpl []
  WfHello
  (hello [_ v]
    (let [activities
          (io.temporal.workflow.Workflow/newActivityStub
           sample-activities/hello-activity
           (-> (io.temporal.activity.ActivityOptions/newBuilder)
               (.setStartToCloseTimeout (java.time.Duration/ofSeconds 10))
               (.setHeartbeatTimeout (java.time.Duration/ofSeconds 1))
               (.build)))]
      (prn :wf-hello v)
      (.hello activities (str "passed from wf: " v)))
    v))

(def wf-hello WfHello)
(def wf-hello-impl WfHelloImpl)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(definterface ^{io.temporal.workflow.WorkflowInterface []} WfMath
  (^{io.temporal.workflow.WorkflowMethod {:name "run"}} run [_params]))

(defrecord WfMathImpl []
  WfMath
  (run [_ {:strs [op] :as params}]
    (let [math
          (io.temporal.workflow.Workflow/newActivityStub
           sample-activities/simple-math-activities
           (-> (io.temporal.activity.ActivityOptions/newBuilder)
               (.setStartToCloseTimeout (java.time.Duration/ofSeconds 10))
               (.build)))]
      (case op
        "add"      (.add math (select-keys params ["args"]))
        "delayAdd" (.delayAdd math (select-keys params ["args" "opts"]))))))

(def wf-math WfMath)
(def wf-math-impl WfMathImpl)
