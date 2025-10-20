(ns lotuc.sample.temporal.sample-ignored
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.common :as common]))

(definterface ^{io.temporal.activity.ActivityInterface []}
  AutoFileCacheActivity
  (^{io.temporal.activity.ActivityMethod {:name "auto-file-cache/download-file"}}
   downloadFile [v])
  (^{io.temporal.activity.ActivityMethod {:name "auto-file-cache/upload-file"}}
   uploadFile [v]))

(definterface ^{io.temporal.activity.ActivityInterface []}
  AutoD3Activity
  (^{io.temporal.activity.ActivityMethod {:name "auto-d3/convert-to-stl"}}
   convertToStl [v])
  (^{io.temporal.activity.ActivityMethod {:name "auto-d3/stl-read-admesh"}}
   stlReadAdmesh [v])
  (^{io.temporal.activity.ActivityMethod {:name "auto-d3/stl-read"}}
   stlRead [v])
  (^{io.temporal.activity.ActivityMethod {:name "auto-d3/stl-check"}}
   stlCheck [v])
  (^{io.temporal.activity.ActivityMethod {:name "auto-d3/calc-wall-thickness"}}
   calcWallThickness [v])
  (^{io.temporal.activity.ActivityMethod {:name "auto-d3/stl-orientation-min-base-area-and-height-box"}}
   stlOrientationMinBaseAreaAndHeightBox [v])
  (^{io.temporal.activity.ActivityMethod {:name "auto-d3/stl-thumb"}}
   stlThumb [v]))

(definterface ^{io.temporal.workflow.WorkflowInterface []}
  WfAutoTest
  (^{io.temporal.workflow.WorkflowMethod []} run [_params]))

(defrecord WfAutoTestImpl []
  WfAutoTest
  (run [_ {:strs [method params]}]
    (let [file-cache
          (io.temporal.workflow.Workflow/newActivityStub
           AutoFileCacheActivity
           (-> (io.temporal.activity.ActivityOptions/newBuilder)
               (.setTaskQueue "auto-file-cache")
               (.setStartToCloseTimeout (java.time.Duration/ofMinutes 30))
               (.setHeartbeatTimeout (java.time.Duration/ofSeconds 10))
               (.setRetryOptions
                (-> (io.temporal.common.RetryOptions/newBuilder)
                    (.setMaximumAttempts 1)
                    (.build)))
               (.build)))

          auto-d3
          (io.temporal.workflow.Workflow/newActivityStub
           AutoD3Activity
           (-> (io.temporal.activity.ActivityOptions/newBuilder)
               (.setTaskQueue "auto-d3")
               (.setStartToCloseTimeout (java.time.Duration/ofMinutes 30))
               (.setHeartbeatTimeout (java.time.Duration/ofSeconds 10))
               (.setRetryOptions
                (-> (io.temporal.common.RetryOptions/newBuilder)
                    (.setMaximumAttempts 1)
                    (.build)))
               (.build)))]

      (case method
        "auto-file-cache/download-file" (.downloadFile file-cache params)
        "auto-file-cache/upload-file" (.uploadFile file-cache params)
        "auto-d3/convert-to-stl" (.convertToStl auto-d3 params)
        "auto-d3/stl-read-admesh" (.stlReadAdmesh auto-d3 params)
        "auto-d3/stl-read" (.stlRead auto-d3 params)
        "auto-d3/stl-check" (.stlCheck auto-d3 params)
        "auto-d3/calc-wall-thickness" (.calcWallThickness auto-d3 params)
        "auto-d3/stl-orientation-min-base-area-and-height-box" (.stlOrientationMinBaseAreaAndHeightBox auto-d3 params)
        "auto-d3/stl-thumb" (.stlThumb auto-d3 params)))))

(def task-queue "auto-test-wf")

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [WfAutoTestImpl]}}))

(defn wf-options [workflow-id]
  (-> (io.temporal.client.WorkflowOptions/newBuilder)
      (.setWorkflowId workflow-id)
      (.setTaskQueue task-queue)
      (.build)))

(defn create-&-run-workflow
  [workflow-id params]
  (future
    (-> (common/default-client)
        (.newWorkflowStub WfAutoTest (wf-options workflow-id))
        (.run params))))

(comment

  (def stl-md5 "b820dd64fab23aba3ffb99964bed60b7")

  (def a (create-&-run-workflow
          (str "download-" stl-md5)
          {"method" "auto-file-cache/download-file"
           "params" {"url" "http://localhost:8000/aa.stl"
                     "md5" stl-md5}}))

  (def b (create-&-run-workflow
          "stl-thumb-b820dd64fab23aba3ffb99964bed60b7"
          {"method" "auto-d3/stl-thumb"
           "params" {"md5" stl-md5}}))

  (def c (create-&-run-workflow
          "stl-read-b820dd64fab23aba3ffb99964bed60b7"
          {"method" "auto-d3/stl-read"
           "params" {"md5" stl-md5}}))

  (def d (create-&-run-workflow
          "stl-check-b820dd64fab23aba3ffb99964bed60b7"
          {"method" "auto-d3/stl-check"
           "params" {"md5" stl-md5}}))

  (def e (create-&-run-workflow
          "calc-wall-thickness-b820dd64fab23aba3ffb99964bed60b7"
          {"method" "auto-d3/calc-wall-thickness"
           "params" {"md5" stl-md5}}))

  (def f (create-&-run-workflow
          "stl-orientation-min-base-area-and-height-box-b820dd64fab23aba3ffb99964bed60b7"
          {"method" "auto-d3/stl-orientation-min-base-area-and-height-box"
           "params" {"md5" stl-md5}}))

  a
  b
  c

  #_())
