(ns lotuc.sample.temporal.sample01-wf-102
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.common
    :refer [activity-execution-ctx activity-log workflow-info workflow-log]
    :as common]))

;;; https://docs.temporal.io/develop/java/message-passing
;;; https://docs.temporal.io/develop/java/cancellation
;;; https://docs.temporal.io/develop/java/failure-detection
;;; https://docs.temporal.io/develop/java/asynchronous-activity-completion
;;;   - https://docs.temporal.io/activity-execution#asynchronous-activity-completion
;;; https://docs.temporal.io/develop/java/versioning

;;; **Parameters default encode to JSON.** Using string keys.

(defn send-email! [ctx {:strs [email message count]}]
  (let [t0 (System/currentTimeMillis)]
    (try
      (activity-log ctx (format "[.] sending to %s with message: %s, count %s" email message count))
      (let [n (rand-int 3)]             ; randomize total cost time
        (doseq [i (range n)]
          (if (< (rand) 0.3)          ; randomize heartbeat
            (do (.heartbeat ctx {:hb i :n n})
                (activity-log ctx (format " ... hb sent %s" i)))
            (activity-log ctx (format " ... hb skipped %s" i)))
          (Thread/sleep 1000))
        (.heartbeat ctx {:n n :reason "checking aliveness"}))
      (activity-log ctx (format "[o] sent to %s with message: %s, count %s, costs %s"
                                email message count (- (System/currentTimeMillis) t0)))
      (catch io.temporal.client.ActivityCompletionException e
       ;; the heartbeat lost could tirgger such a exception.
        (->> (format "[!] CANCELLED sending to %s with message: %s, count %s, costs %s\n     ! %s"
                     email message count (- (System/currentTimeMillis) t0)
                     (str e))
             (activity-log ctx))
        (throw e)))))

(defn async-send-email! [ctx params]
  (future
    (let [task-token (.getTaskToken ctx)
          completion-client (.newActivityCompletionClient (.getWorkflowClient ctx))]
      (try (send-email! ctx params)
           (.complete completion-client task-token "success")
           (catch Throwable t
             (.completeExceptionally​ completion-client task-token t))))))

(definterface ^{io.temporal.activity.ActivityInterface []}
  SendEmailActivities
  (^{io.temporal.activity.ActivityMethod {:name "send-email"}}
   sendEmail [details])
  (^{io.temporal.activity.ActivityMethod {:name "async-send-email"}}
   asyncSendEmail [details]))

(defrecord SendEmailActivitiesImpl []
  SendEmailActivities
  (sendEmail [_ params]
    (let [ctx (activity-execution-ctx)]
      (send-email! ctx params))
    "success")
  (asyncSendEmail [_ params]
    (let [ctx (activity-execution-ctx)]
      (async-send-email! ctx params)
      (.doNotCompleteOnReturn ctx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(definterface ^{io.temporal.workflow.WorkflowInterface []}
  SendEmailWorkflow
  (^{io.temporal.workflow.WorkflowMethod []} run [data])
  (^{io.temporal.workflow.QueryMethod []} details [])
  (^{io.temporal.workflow.SignalMethod []} ^void pause [])
  (^{io.temporal.workflow.SignalMethod []} ^void resume []))

(defonce !send-email-workflow-state (atom {}))

(def ^:dynamic *send-email-version* :v0)

(defn wf-send-email*
  [{:keys [this-id activities duration]} {:keys [email async n]}]
  (letfn [(sendv0 [email-details]
            ;; v0: only supports sync method
            (.sendEmail activities email-details))
          (sendv1 [email-details]
            ;; v1: migrate from v0, supports async method
            (let [ver (io.temporal.workflow.Workflow/getVersion
                       "async-added"
                       io.temporal.workflow.Workflow/DEFAULT_VERSION
                       1)]
              (workflow-log (workflow-info)
                            (format "impl version: %s, wf version of async-added: %s"
                                    *send-email-version* ver))
              (if (= ver io.temporal.workflow.Workflow/DEFAULT_VERSION)
                (.sendEmail activities email-details)
                (do
                  ;; Update filter params (workflow list UI filter options).
                  (io.temporal.workflow.Workflow/upsertTypedSearchAttributes
                   (into-array io.temporal.common.SearchAttributeUpdate
                               [(-> (io.temporal.common.SearchAttributeKey/forKeywordList
                                     "TemporalChangeVersion")
                                    (.valueSet [(str "async-added-" ver)]))]))
                  (if async
                    (.asyncSendEmail activities email-details)
                    (.sendEmail activities email-details))))))
          (send* [email-details]
            (case *send-email-version*
              :v0 (sendv0 email-details)
              :v1 (sendv1 email-details)))]
    (let [n' (inc n)
          email-details
          {"email"   email
           "count"   n'
           "message" (if (zero? n) "welcome" "thanks staying")}]
      (let [t0 (io.temporal.workflow.Workflow/currentTimeMillis)]
        (workflow-log (workflow-info) "checking & await pause state")
        (io.temporal.workflow.Workflow/await
         (fn [] (not (get-in @!send-email-workflow-state [this-id :pause]))))
        (let [t1 (io.temporal.workflow.Workflow/currentTimeMillis)]
          (workflow-log (workflow-info) "checked pause state, costs: " (- t1 t0))))
      (try
        (swap! !send-email-workflow-state assoc-in [this-id :email-details] email-details)
        (send* email-details)

        ;; Use timer for UI enriching
        (-> (io.temporal.workflow.Workflow/newTimer
             (java.time.Duration/ofSeconds duration)
             (-> (io.temporal.workflow.TimerOptions/newBuilder)
                 (.setSummary "wait for next sending")
                 (.build)))
            (.get))

        ;; or a simple sleep
        ;; (io.temporal.workflow.Workflow/sleep (java.time.Duration/ofSeconds duration))

        (catch io.temporal.failure.CanceledFailure e
          (let [email-details {"email" email "message" "bye" "count" n}
                send-goodbye (io.temporal.workflow.Workflow/newDetachedCancellationScope
                              (fn [] (send* email-details)))]
            (swap! !send-email-workflow-state assoc-in [this-id :email-details] email-details)
            (.run send-goodbye)
            (throw e))))
      n')))

(defn wf-send-email [this-id {:strs [email async]}]
  (let [duration 3
        activities
        (io.temporal.workflow.Workflow/newActivityStub
         SendEmailActivities
         (-> (io.temporal.activity.ActivityOptions/newBuilder)
             (.setStartToCloseTimeout (java.time.Duration/ofSeconds 10))
             (.setHeartbeatTimeout (java.time.Duration/ofSeconds 1))
             (.build)))
        ctx {:this-id this-id :activities activities :duration duration}]
    (try
      (loop [n 0]
        (let [n (or (get-in @!send-email-workflow-state [this-id :email-details :count]) n)]
          (recur (wf-send-email* ctx {:email email :async async :n n}))))
      (finally
        (swap! !send-email-workflow-state dissoc this-id)))))

(defrecord SendEmailWorkflowImpl []
  SendEmailWorkflow
  (run [this params]
    (let [this-id (System/identityHashCode this)]
      (workflow-log (workflow-info) (format "started with %s" :v0))
      (try
        (binding [*send-email-version* :v0]
          (wf-send-email this-id params))
        (finally
          (swap! !send-email-workflow-state dissoc this-id)))))
  (pause [this]
    (workflow-log (workflow-info) "accept pause signal")
    (let [this-id (System/identityHashCode this)]
      (swap! !send-email-workflow-state assoc-in [this-id :pause] true)))
  (resume [this]
    (workflow-log (workflow-info) "accept resume signal")
    (let [this-id (System/identityHashCode this)]
      (swap! !send-email-workflow-state assoc-in [this-id :pause] false)))
  (details [this]
    (let [this-id (System/identityHashCode this)]
      (get-in @!send-email-workflow-state [this-id :email-details]))))

;;; duplicate only for demo, in real life, could just do update on old code path.
(defrecord SendEmailWorkflowV1Impl []
  SendEmailWorkflow
  (run [this params]
    (let [this-id (System/identityHashCode this)]
      (workflow-log (workflow-info) (format "started with %s" :v1))
      (try
        (binding [*send-email-version* :v1]
          (wf-send-email this-id params))
        (finally
          (swap! !send-email-workflow-state dissoc this-id)))))
  (pause [this]
    (workflow-log (workflow-info) "accept pause signal")
    (let [this-id (System/identityHashCode this)]
      (swap! !send-email-workflow-state assoc-in [this-id :pause] true)))
  (resume [this]
    (workflow-log (workflow-info) "accept resume signal")
    (let [this-id (System/identityHashCode this)]
      (swap! !send-email-workflow-state assoc-in [this-id :pause] false)))
  (details [this]
    (let [this-id (System/identityHashCode this)]
      (get-in @!send-email-workflow-state [this-id :email-details]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^{:doc "queue for testing"} task-queue (str ::queue))

(defn wf-options [workflow-id]
  (-> (io.temporal.client.WorkflowOptions/newBuilder)
      (.setWorkflowId workflow-id)
      (.setTaskQueue task-queue)
      (.build)))

(defn create-&-run-workflow
  [workflow-id params]
  (future
    (-> (common/default-client)
        (.newWorkflowStub SendEmailWorkflow (wf-options workflow-id))
        (.run params))))

(defn create-&-async-run-workflow
  [workflow-id params]
  (io.temporal.client.WorkflowClient/start
   (reify io.temporal.workflow.Functions$Proc1
     (apply [_ v]
       (-> (common/default-client)
           (.newWorkflowStub SendEmailWorkflow (wf-options workflow-id))
           (.run v))))
   params))

(defn terminate-wf-by-id [wf-id]
  (-> (common/default-client)
      (.newWorkflowStub SendEmailWorkflow wf-id)
      (io.temporal.client.WorkflowStub/fromTyped)
      (.terminate "from repl" (into-array Object []))))

(def wf-version :v0)

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client  {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes
     [(case wf-version
        :v0 SendEmailWorkflowImpl
        :v1 SendEmailWorkflowV1Impl)]
     :activity-instances
     [(SendEmailActivitiesImpl.)]}}))

(comment

  (def wf42-run (->> {"email" "lotu.c@outlook.com" "async" true}
                     (create-&-run-workflow "wf42")))
  (def wf43-run (->> {"email" "lotu.c@outlook.com" "async" true}
                     (create-&-async-run-workflow "wf43")))

  (def wf42-stub
    (.newWorkflowStub (common/default-client) SendEmailWorkflow "wf42"))

  ;; signals
  (.pause wf42-stub)
  (.resume wf42-stub)

  ;; query method
  (.details wf42-stub)

  ;; cancel | terminate
  (def wf42-stub-untyped
    (io.temporal.client.WorkflowStub/fromTyped wf42-stub))
  (.cancel wf42-stub-untyped)
  (.terminate wf42-stub-untyped "from repl" (into-array Object []))

  (do (terminate-wf-by-id "wf42")
      (terminate-wf-by-id "wf43"))

  #_())
