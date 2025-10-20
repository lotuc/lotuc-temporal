(ns lotuc.sample.temporal.sample07-sci-102
  (:require
   [clojure.core.async :as async]
   [clojure.edn :as edn]
   [hyperfiddle.rcf :as rcf]
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.common :as common]
   [lotuc.sci-rt.temporal.activity :as temporal.activity]
   [lotuc.sci-rt.temporal.sci :as temporal.sci]
   [lotuc.sci-rt.temporal.workflow :as temporal.workflow]
   [lotuc.temporal.sci :refer [with-sci-wf]]
   [sci.core :as sci]
   [taoensso.telemere :as tel]))

(def task-queue "hello")

(defn hello [msg]
  (prn [:hello msg]))

(def !store (atom nil))

(defn store! [v]
  (reset! !store v))

(defn env-vars []
  {:activity-options (temporal.workflow/get-activity-options)})

(defn long-task-with-heartbeat [{:keys [hb-interval task-ms]}]
  (let [!hb-error (atom nil)
        ctx (temporal.activity/execution-ctx)
        close-ch  (async/chan)
        hb
        (fn []
          (async/thread
            (tel/log! {:msg ["heartbeat" hb-interval]})
            (try (.heartbeat ctx "long-task")
                 true
                 (catch Throwable t
                   (reset! !hb-error t)
                   (async/close! close-ch)
                   false))))
        task-ch
        (async/go
          (async/<! (async/timeout task-ms))
          "done")]

    ;; heartbeat ch
    (async/go-loop []
      (when (async/alt!
              close-ch ([_] false)
              task-ch  ([_] false)
              (hb)     ([v] v))
        (async/<! (async/timeout hb-interval))
        (recur)))

    (let [v (async/alt!!
              task-ch  ([v] v)
              close-ch ([_] "timeout"))]
      (if-some [err @!hb-error]
        (throw err)
        v))))

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [lotuc.temporal.sci.SciWorkflowImpl]
     :activity-instances
     [(lotuc.temporal.sci.SciActivityImpl.
       {:namespaces
        (fn [] {'sample
                {'hello hello
                 'long-task-with-heartbeat long-task-with-heartbeat}})})]}}))

(defn run-options
  ([]
   (run-options (str (random-uuid))))
  ([wf-id]
   ;; debug
   #_{:clj-kondo/ignore [:inline-def]}
   (def wf-id wf-id)
   {:workflow-service-stubs @common/+default-service-stubs+
    :workflow-options {:taskQueue task-queue :workflowId wf-id}}))

(defn message-options
  ([wf-id]
   {:workflow-service-stubs @common/+default-service-stubs+
    :workflow-id wf-id}))

(rcf/tests
 (try (temporal.workflow/rethrow-inside-retry
       (ex-info "retryable" {:temporal/retryable false}))
      (catch Throwable t (type t)))
 := lotuc.sci_rt.temporal.DoNotRetryExceptionInfo

 (try (temporal.workflow/rethrow-inside-retry
       (try (sci/eval-string
             (pr-str '(throw (ex-info "retryable" {:temporal/retryable false}))))
            (catch Throwable e e)))
      (catch Throwable t (type t)))
 := lotuc.sci_rt.temporal.DoNotRetryExceptionInfo)

(rcf/enable!)
(integrant.repl/halt)
(integrant.repl/go)

(def v0 (with-sci-wf [_ (run-options)]
          (+ 1 2)))

(def v1 (try (with-sci-wf [_ (run-options)]
               (time (/ 1 0)))
             (catch Throwable e e)))

(def v2 (let [t0 (System/currentTimeMillis)]
          (try (with-sci-wf [_ (run-options)]
                 (temporal.workflow/with-retry {:initialInterval [:sec 1]
                                                :expiration [:sec 4]}
                   (/ 1 0)))
               (catch Throwable t [t (- (System/currentTimeMillis) t0)]))))

;;; async
(def v2' (let [t0 (System/currentTimeMillis)]
           (try (with-sci-wf [_ (run-options)]
                  @(temporal.workflow/with-retry-async {:initialInterval [:sec 1]
                                                        :expiration [:sec 4]}
                     (/ 1 0)))
                (catch Throwable t [t (- (System/currentTimeMillis) t0)]))))

(def v3 (let [t0 (System/currentTimeMillis)]
          (try (with-sci-wf [_ (run-options)]
                 (temporal.workflow/with-retry {:initialInterval [:sec 3]
                                                :expiration [:sec 10]}
                   ;; explicitly marked as not retryable
                   (try (/ 1 0) (catch Exception e
                                  (throw (ex-info (ex-message e) {:temporal/retryable false} e))))))
               (catch Throwable t [t (- (System/currentTimeMillis) t0)]))))

(def v3' (let [t0 (System/currentTimeMillis)]
           (try (with-sci-wf [_ (run-options)]
                  @(temporal.workflow/with-retry-async {:initialInterval [:sec 3]
                                                        :expiration [:sec 10]}
                   ;; explicitly marked as not retryable
                     (try (/ 1 0) (catch Exception e
                                    (throw (ex-info (ex-message e) {:temporal/retryable false} e))))))
                (catch Throwable t [t (- (System/currentTimeMillis) t0)]))))

(def v4
  [(with-sci-wf [_ (run-options)]
     {:namespaces {'sample {'store! (str `store!)}}}
     (sample/store! 42))
   @!store])

(def v5
  (edn/read-string
   (with-sci-wf [_ (run-options)]
     {:namespaces {'sample {'env-vars (str `env-vars)}}}
     (pr-str (sample/env-vars)))))

(def v6
  (edn/read-string
   (with-sci-wf [_ (run-options)]
     {:namespaces {'sample {'env-vars (str `env-vars)}}}
     (temporal.workflow/with-activity-options {:startToCloseTimeout 42}
       (pr-str [temporal.workflow/activity-options
                (:activity-options (sample/env-vars))])))))

(rcf/tests
 v0 := 3
 (instance? io.temporal.client.WorkflowFailedException v1) := true

 ;; default retry backoff strategy: 1s, 2s, 4s, ...

 (instance? io.temporal.client.WorkflowFailedException (v2 0)) := true
 (> (v2 1) 3000) := true

 (instance? io.temporal.client.WorkflowFailedException (v2' 0)) := true
 (> (v2' 1) 3000) := true

 (instance? io.temporal.client.WorkflowFailedException (v3 0)) := true
 (< (v3 1) 1000) := true

 (instance? io.temporal.client.WorkflowFailedException (v3' 0)) := true
 (< (v3' 1) 1000) := true

 v4 := [42 42]

 ;; default activity options
 v5 := {:activity-options {:startToCloseTimeout [:sec 60]}}

 ;; TODO:
 (v6 0) := (v6 1))

(comment

  (with-sci-wf [_ (run-options)]
    (time (+ 1 2)))

  (with-sci-wf [_ (run-options)]
    {:namespaces {'sample {'hello (str `hello)}}}
    (sample/hello "world"))

  #_(with-sci-wf [_ (run-options)]
      (temporal.workflow/with-activity-options {:startToCloseTimeout [:sec 60]
                                                :heartbeatTimeout [:sec 1]}
        (prn temporal.workflow/activity-options)))

  (with-sci-wf [_ (run-options)]
    (time (temporal.sci/sleep 1000)))

  (with-sci-wf [_ (run-options)]
    (time (temporal.sci/sleep 1000)))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; when *NOT* handling workflow execution task's retry explicitly

  ;; exception defaults be *not retryable*
  (with-sci-wf [_ (run-options)]
    (time (/ 1 0)))

  ;; this one will be retried by temporal itself forever.
  (with-sci-wf [_ (run-options)]
    (try (time (/ 1 0))
         (catch Exception e
           (throw (ex-info (ex-message e) {:temporal/retryable true} e)))))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; when *DO* handling workflow execution task's retry explicitly

  ;; exception defaults be *retryable*
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-retry {:initialInterval [:sec 3]
                                   :expiration [:sec 10]}
      (/ 1 0)))

  ;; unless being marked as not retryable
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-retry {:initialInterval [:sec 3]
                                   :expiration [:sec 10]}
      (try (/ 1 0) (catch Exception e
                     (throw (ex-info (ex-message e) {:temporal/retryable false} e))))))

  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-retry {:expiration [:sec 6]}
      (throw (ex-info "not-retryable" {:temporal/retryable false}))))

  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-retry {:expiration [:sec 6]}
      (throw (ex-info "retryable" {:temporal/retryable true}))))

  ;; async ones

  (with-sci-wf [_ (run-options)]
    @(temporal.workflow/with-retry-async {:initialInterval [:sec 1]
                                          :expiration [:sec 6]}
       (/ 1 0)))

  (with-sci-wf [_ (run-options)]
    @(temporal.workflow/with-retry-async {:initialInterval [:sec 1]
                                          :expiration [:sec 6]}
       (try (/ 1 0)
            (catch Exception e
              (throw (ex-info (ex-message e) {:temporal/retryable false} e))))))

  (with-sci-wf [_ (run-options)]
    @(temporal.workflow/with-retry-async {:initialInterval [:sec 3]
                                          :expiration [:sec 10]}
       (throw (ex-info "not-retryable" {:temporal/retryable false}))))

  #_())

;;; activity
(comment

  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-sci-activity
      (+ 4 2)))

  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-sci-activity
      (sample/hello "world")))

  ;; activity with timeout
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-activity-options
      {:retryOptions {:maximumAttempts 2}
       :startToCloseTimeout [:sec 60]
       :heartbeatTimeout [:sec 1]}
      (temporal.workflow/with-sci-activity
        (prn :run-activity)
        (sample/long-task-with-heartbeat
         ;; 1500ms > 1s, will cause heartbeat timeout
         {:hb-interval 1500 :task-ms 6000}))))

  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-sci-activity {:params {:a 42}}
      (let [{:keys [a]} temporal.activity/params]
        (+ a 24))))

  (with-sci-wf [_ (run-options)]
    (time (temporal.workflow/with-sci-activity
            (temporal.sci/sleep 1000))))

  ;; sci activity defaults to be not retryable
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-sci-activity
      (throw (ex-info "hello" {}))))

  ;; when marked as retryable, activity defaults to retry forever
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-sci-activity
      (throw (ex-info "hello" {:temporal/retryable true}))))

  ;; you can specify activity retry options
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-activity-options
      {:retryOptions {:maximumAttempts 3}
       :startToCloseTimeout [:sec 60]}
      (time (temporal.workflow/with-sci-activity
              (throw (ex-info "hello" {:temporal/retryable true}))))))

  ;; or do retry on a workflow basis
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-retry {:expiration [:sec 6]}
      (temporal.workflow/with-sci-activity
        (throw (ex-info "hello" {:temporal/retryable false})))))

  ;; or combine them
  (with-sci-wf [_ (run-options)]
    (temporal.workflow/with-retry {:expiration [:sec 6]}
      (+
       (temporal.workflow/with-activity-options
         {:retryOptions {:maximumAttempts 3}
          :startToCloseTimeout [:sec 60]}
         (temporal.workflow/with-sci-activity
           (let [v (rand)]
             (when (< v 0.5)
               (throw (ex-info "hello" {:temporal/retryable true})))
             v)))
       (temporal.workflow/with-activity-options
         {:retryOptions {:maximumAttempts 3}
          :startToCloseTimeout [:sec 60]}
         (temporal.workflow/with-sci-activity
           (let [v (rand)]
             (when (< v 0.5)
               (throw (ex-info "hello" {:temporal/retryable true})))
             v))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; async activity

  (with-sci-wf [_ (run-options)]
    @(temporal.workflow/with-sci-activity-async {:params {:a 42}}
       (let [{:keys [a]} temporal.activity/params]
         (+ a 24))))

  (with-sci-wf [_ (run-options)]
    (let [vs (for [i (range 10)]
               (temporal.workflow/with-sci-activity-async {:params {:i i}}
                 (let [{:keys [i]} temporal.activity/params]
                   (when (< (rand) 0.5)
                     (throw (ex-info (str "error: " i) {:temporal/retryable true})))
                   (temporal.sci/sleep (* 1000 (rand-int 10)))
                   i)))]
      (letfn [(rf [r i] (+ r @i))]
        (time (reduce rf 0 (doall vs))))))

  #_())

;;; messaging
(comment

  (do (def !f
        (future (with-sci-wf [_ (run-options "hello")]
                  (do (temporal.sci/sleep 60000)
                      temporal.workflow/state))))
      (def !f1
        (future (Thread/sleep 1500)
                (with-sci-wf [_signal! (message-options "hello")]
                  (doseq [i (range 6)]
                    (temporal.sci/sleep 1000)
                    (prn [temporal.workflow/action-name :add i])
                    (alter-var-root #'temporal.workflow/state
                                    (fnil + 0) i)))))
      #_())

  (with-sci-wf [_signal! (message-options "hello")]
    (prn [temporal.workflow/action-name temporal.workflow/action-params]))

  (with-sci-wf [_update! (message-options "hello")]
    {:validator (fn [_] (prn :validating _))}
    (prn [temporal.workflow/action-name :add 24])
    (alter-var-root #'temporal.workflow/state (fnil + 0) 24))

  ;; if workflow is alive, the query forwards to the running instance
  ;; else if workflow completed, you can see the workflow is replayed
  (with-sci-wf [_query (message-options "hello")]
    (prn [temporal.workflow/action-name :state temporal.workflow/state])
    temporal.workflow/state)

  ;; this should not be allowed
  (with-sci-wf [_query (message-options "hello")]
    (prn [temporal.workflow/action-name :state temporal.workflow/state])
    (alter-var-root #'temporal.workflow/state (constantly 24)))

  #_())
