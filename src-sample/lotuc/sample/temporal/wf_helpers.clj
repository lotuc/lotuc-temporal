(ns lotuc.sample.temporal.wf-helpers
  (:require
   [clojure.walk]
   [pronto.core :as pronto]))

#_{:clj-kondo/ignore [:unused-binding]}
(pronto/defmapper mapper
  [io.temporal.api.common.v1.WorkflowExecution
   io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
   io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse
   io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest
   io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse])

(defn clj-map->proto-map [clazz v]
  (pronto/clj-map->proto-map mapper clazz v))

(defn proto->proto-map [v]
  (pronto/proto->proto-map mapper v))

(defn describe-workflow-execution
  [stubs opts]
  (->> (.describeWorkflowExecution
        (.blockingStub stubs)
        (->> (clj-map->proto-map
              io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
              opts)
             (pronto/proto-map->proto)))
       (pronto/proto->proto-map mapper)))

(defn get-workflow-execution-history [stubs opts]
  (->> (.getWorkflowExecutionHistory
        (.blockingStub stubs)
        (->> (clj-map->proto-map
              io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest
              opts)
             (pronto/proto-map->proto)))
       (proto->proto-map)))

(def workflow-execution-finished-types
  #{:EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
    :EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
    :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
    :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
    :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED})

(defn get-input-from-execution-history
  ([wf-execution-history]
   (get-input-from-execution-history
    wf-execution-history
    (fn [^com.google.protobuf.ByteString v]
      (.toString v "UTF-8"))))
  ([wf-execution-history decode-byte-string]
   (->> (:history wf-execution-history)
        (:events)
        (filter (comp #{:EVENT_TYPE_WORKFLOW_EXECUTION_STARTED} :event_type))
        (first)
        (#(get-in % [:workflow_execution_started_event_attributes
                     :input :payloads]))
        (mapv (fn [v]
                (cond->> (pronto/proto-map->clj-map v)
                  decode-byte-string
                  (clojure.walk/postwalk
                   (fn [v] (if (instance? com.google.protobuf.ByteString v)
                             (decode-byte-string v)
                             v)))))))))

(defn get-result-from-execution-history
  [wf-execution-history]
  (letfn [(parse-result [{:keys [event_type] :as v}]
            {:result-event-type event_type
             :result
             (case event_type
               :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
               (->> v
                    (#(get-in % [:workflow_execution_completed_event_attributes
                                 :result :payloads]))
                    (mapv (fn [v]
                            (clojure.walk/postwalk
                             (fn [v] (if (instance? com.google.protobuf.ByteString v)
                                       (.toString v "UTF-8")
                                       v))
                             (pronto/proto-map->clj-map v)))))
               :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
               (:workflow_execution_terminated_event_attributes v)
               ;; TODO:
               )})]
    (->> (:history wf-execution-history)
         (:events)
         (filter (comp workflow-execution-finished-types :event_type))
         (first)
         (parse-result))))

(comment

  (def stubs (io.temporal.serviceclient.WorkflowServiceStubs/newLocalServiceStubs))
  (def wf-execution
    (describe-workflow-execution stubs {:namespace "default" :execution {:workflow_id "wf000"}}))
  (def wf-execution-history
    (get-workflow-execution-history
     stubs {:namespace "default" :execution {:workflow_id "wf000"}
            :history_event_filter_type
            :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}))

  (get-input-from-execution-history wf-execution-history)
  (get-result-from-execution-history wf-execution-history)

  (->> (get-workflow-execution-history
        stubs {:namespace "default"
               :execution {:workflow_id "91260fd1-70a0-47fd-8c6b-5282d77f263b"}})
       (get-result-from-execution-history))

  (->> (get-workflow-execution-history
        stubs {:namespace "default"
               :execution {:workflow_id "wf002"}})
       (get-result-from-execution-history))

  #_())
