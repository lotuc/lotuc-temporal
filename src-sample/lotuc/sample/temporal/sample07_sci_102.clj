(ns lotuc.sample.temporal.sample07-sci-102
  (:require
   [clojure.core.async :as async]
   [clojure.edn :as edn]
   [hyperfiddle.rcf :as rcf]
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [integrant.repl.state :as ig.repl.state]
   [lotuc.sample.temporal.common :as common]
   [lotuc.sci-rt.temporal.activity :as temporal.activity]
   [lotuc.sci-rt.temporal.ex :as temporal.ex]
   [lotuc.sci-rt.temporal.sci :as temporal.sci]
   [lotuc.sci-rt.temporal.workflow :as temporal.workflow]
   [lotuc.temporal.sci :refer [with-sci-code]]
   [sci.core :as sci]
   [taoensso.telemere :as tel]))

(def task-queue "hello")

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [lotuc.temporal.sci.SciWorkflowImpl]
     :activity-instances
     [(lotuc.temporal.sci.SciActivityImpl. {:namespaces {}})]}}))

(defn client []
  (:sample/default-client ig.repl.state/system))

(defn terminate-workflow [id]
  (.terminate (io.temporal.client.WorkflowClient/.newUntypedWorkflowStub (client) id)
              (str `testing) (into-array Object [])))

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

(rcf/enable!)

(rcf/tests
 "within retry block, exceptions marked as `:temporal/retryable` falsy will be
rethrown as lotuc.sci_rt.temporal.DoNotRetryExceptionInfo"

 (= (try (temporal.workflow/rethrow-inside-retry
          (ex-info "retryable" {:temporal/retryable false}))
         (catch Throwable t (type t)))
    lotuc.sci_rt.temporal.DoNotRetryExceptionInfo)
 := true

 (= (try (temporal.workflow/rethrow-inside-retry
          (try (sci/eval-string
                (pr-str '(throw (ex-info "retryable" {:temporal/retryable false}))))
               (catch Throwable e e)))
         (catch Throwable t (type t)))
    lotuc.sci_rt.temporal.DoNotRetryExceptionInfo)

 #_())

(rcf/tests
 "top level exception should be NOT RETRYABLE unless EXPLICITLY marked as is"

 (= (try (temporal.ex/rethrow-toplevel (java.lang.ArithmeticException.))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := true

 (= (try (temporal.ex/rethrow-toplevel (ex-info "" {:temporal/retryable false}))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := true

 ;; check the "actual cause" with `tempora.ex/actual-cause`
 (= (try (try (sci/eval-string
               (with-sci-code
                 (try (time (/ 1 0))
                      (catch Exception e
                        (throw (ex-info (ex-message e) {:temporal/retryable false} e))))))
              (catch Throwable t (temporal.ex/rethrow-toplevel t)))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := true

 (= (try (try (sci/eval-string
               (with-sci-code
                 (time (/ 1 0))))
              (catch Throwable t (temporal.ex/rethrow-toplevel t)))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := true

 (= (try (temporal.ex/rethrow-toplevel (ex-info "" {}))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := true

 ;; retryable

 (= (try (try (sci/eval-string
               (with-sci-code
                 (try (time (/ 1 0))
                      (catch Exception e
                        (throw (ex-info (ex-message e) {:temporal/retryable true} e))))))
              (catch Throwable t (temporal.ex/rethrow-toplevel t)))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := false

 (= (try (temporal.ex/rethrow-toplevel (ex-info "" {:temporal/retryable true}))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := false

 ;; :temporal/retryable supercedes given retryable check function
 (= (try (temporal.ex/rethrow-toplevel (ex-info "" {:temporal/retryable true})
                                       (fn [_e] false))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := false

 (= (try (temporal.ex/rethrow-toplevel (ex-info "" {}) (fn [_e] false))
         (catch Throwable t (type t)))
    io.temporal.failure.ApplicationFailure)
 := true

 ;; TemporalException retrhown as IS
 (= (try (temporal.ex/rethrow-toplevel
          (io.temporal.failure.TimeoutFailure. nil nil nil)
          (fn [_e] false))
         (catch Throwable t (type t)))
    io.temporal.failure.TimeoutFailure)
 := true

 #_())

(integrant.repl/halt)
(integrant.repl/go)

(rcf/set-timeout! 1000)

(defn ^{:style/indent 1} sci-run*
  ([params] (sci-run* wf-id params))
  ([wf-id params]
   (lotuc.temporal.sci/sci-run!
    (lotuc.temporal.sci/sci-workflow-stub (run-options wf-id))
    params)))

(rcf/tests
 "workflow code execution"

 (lotuc.temporal.sci/sci-run!
  (lotuc.temporal.sci/sci-workflow-stub (run-options))
  {:code "(+ 4 2)"})
 := 6

 (sci-run* {:code "(+ 4 2)"}) := 6

 (sci-run* {:code (with-sci-code (+ 4 2))}) := 6)

(binding [rcf/*timeout* 2000]
  (rcf/tests
   "dynamically load namesapces"

   (defn add2 [a b] (+ a b))

   (sci-run* {:code "(user/add2 4 2)"
              :namespaces {'user {'add2 (str `add2)}}}) := 6

   ;; state on host env defaults be nil
   temporal.workflow/state := nil

   (defn get-state [] temporal.workflow/state)

   (defn f []
     (sci-run*
      {:namespaces {'user {'get-state (str `get-state)}}
       :code
       #_{:clj-kondo/ignore [:unresolved-namespace]}
       (with-sci-code
         (temporal.sci/sleep 1000)
         (alter-var-root #'temporal.workflow/state (fnil + 1) 41)
         [temporal.workflow/state
          (user/get-state)])}))
   ;; concurrent runs does interere with each other
   (future (rcf/tap (f)))
   (future (rcf/tap (f)))
   rcf/% := [42 42]
   rcf/% := [42 42]
   ;; state on host being untouched
   temporal.workflow/state := nil))

(rcf/tests
 "passing params to workflow"

  ;; the code runs on some worker, parameters pass along with the code &
  ;; initialized to `temporal.workflow/params` var when evaluating the code.
 (sci-run* {:code (with-sci-code temporal.workflow/params)
            :params 42})
 := 42

  ;; if the code returns a function, the whole workflow input will be passed in
  ;; to that function, and the function's response will be the workflow's
  ;; response.
 (def !v (atom nil))
 (defn set-v! [v] (reset! !v v))
 (def code (with-sci-code
             (fn [{:keys [params code]}]
               (set-v! code)
               (= params temporal.workflow/params))))
 (sci-run* {:code code
            :namespaces {'user {'set-v! (str `set-v!)}}
            :params (str (random-uuid))})
 := true
 @!v := code)

(binding [rcf/*timeout* 1000]
  (rcf/tests
   "exceptions under workflow execution defaults to be not retryable"

   (def wf-id (str (random-uuid)))
   (future
     (try (sci-run* wf-id {:code (with-sci-code (time (/ 1 0)))})
          (catch Throwable _)
          (finally (rcf/tap :done))))
   rcf/% := :done)

  (rcf/tests
   "exception marked as `retryable` make workflow execution retries"

   (def wf-id (str (random-uuid)))
   (future
     (try (sci-run* wf-id
            {:code (with-sci-code
                     (try (time (/ 1 0))
                          (catch Exception e
                            (throw (ex-info "" {:temporal/retryable true})))))})
          (catch Throwable _)
          (finally (rcf/tap :done))))
   rcf/% := ::rcf/timeout
   (terminate-workflow wf-id)))

(binding [rcf/*timeout* 6000]
  (rcf/tests
   "manually setup retry for workflow execution"

   (def !n (atom 0))
   (defn inc-n! [] (swap! !n inc))

   (binding [rcf/*timeout* 6000]
     (rcf/tests
      "with-retry for retryable"
      (def wf-id (str (random-uuid)))

      (reset! !n 0)
      (future (let [t0 (System/currentTimeMillis)]
                (try (sci-run* wf-id
                       {:namespaces {'user {'inc-n! (str `inc-n!)}}
                        :code
                        #_{:clj-kondo/ignore [:unresolved-namespace]}
                        (with-sci-code
                          (temporal.workflow/with-retry {:initialInterval [:ms 100]
                                                         :backoffCoefficient 1
                                                         :expiration [:sec 4]
                                                         :maximumAttempts 3}
                            (user/inc-n!)
                            (/ 1 0)))})
                     (catch Throwable t
                       (rcf/tap [(instance? io.temporal.client.WorkflowFailedException t)
                                 (> (- (System/currentTimeMillis) t0) 300)
                                 @!n])))))
      rcf/% := [true true 3]

      (try (terminate-workflow wf-id) (catch Throwable _))))

   (rcf/tests
    "with-retry-async for retryable"

    (def wf-id (str (random-uuid)))

    (reset! !n 0)
    (future (let [t0 (System/currentTimeMillis)]
              (try (sci-run* wf-id
                     {:namespaces {'user {'inc-n! (str `inc-n!)}}
                      :code
                      #_{:clj-kondo/ignore [:unresolved-namespace]}
                      (with-sci-code
                        @(temporal.workflow/with-retry-async {:initialInterval [:ms 100]
                                                              :backoffCoefficient 1
                                                              :expiration [:sec 4]
                                                              :maximumAttempts 3}
                           (user/inc-n!)
                           (/ 1 0)))})
                   (catch Throwable t
                     (rcf/tap [(instance? io.temporal.client.WorkflowFailedException t)
                               (> (- (System/currentTimeMillis) t0) 300)
                               @!n])))))
    rcf/% := [true true 3]

    (try (terminate-workflow wf-id) (catch Throwable _)))

   (rcf/tests
    "with-retry manually mark exception as non-retryable"
    (reset! !n 0)
    (future (try (sci-run*
                  {:namespaces {'user {'inc-n! (str `inc-n!)}}
                   :code
                   #_{:clj-kondo/ignore [:unresolved-namespace]}
                   (with-sci-code
                     (temporal.workflow/with-retry {:initialInterval [:sec 3]
                                                    :expiration [:sec 10]}
                       (user/inc-n!)
                       (try (/ 1 0)
                            (catch Exception e
                              (throw (ex-info (ex-message e) {:temporal/retryable false} e))))))})
                 (catch Throwable t
                   (rcf/tap [(instance? io.temporal.client.WorkflowFailedException t) 1]))))
    rcf/% := [true 1])

   (rcf/tests
    "with-retry-async manually mark exception as non-retryable"
    (reset! !n 0)
    (future (try (sci-run*
                  {:namespaces {'user {'inc-n! (str `inc-n!)}}
                   :code
                   #_{:clj-kondo/ignore [:unresolved-namespace]}
                   (with-sci-code
                     @(temporal.workflow/with-retry-async {:initialInterval [:sec 3]
                                                           :expiration [:sec 10]}
                        (user/inc-n!)
                        (try (/ 1 0)
                             (catch Exception e
                               (throw (ex-info (ex-message e) {:temporal/retryable false} e))))))})
                 (catch Throwable t
                   (rcf/tap [(instance? io.temporal.client.WorkflowFailedException t) 1]))))
    rcf/% := [true 1])))

(defn env-vars []
  {:activity-options temporal.workflow/activity-options})

(rcf/tests
 "bound value is passed back to native env."

 (edn/read-string
  #_{:clj-kondo/ignore [:unresolved-namespace]}
  (sci-run*
   {:namespaces {'sample {'env-vars (str `env-vars)}}
    :code (with-sci-code (pr-str (sample/env-vars)))}))
 := {:activity-options {:startToCloseTimeout [:sec 60]}}

 (edn/read-string
  #_{:clj-kondo/ignore [:unresolved-namespace]}
  (sci-run*
   {:namespaces {'sample {'env-vars (str `env-vars)}}
    :code (with-sci-code
            (temporal.workflow/with-activity-options {:startToCloseTimeout 42}
              (pr-str (sample/env-vars))))}))

 := {:activity-options {:startToCloseTimeout 42}})

(rcf/tests
 "heartbeat"

 (def !count (atom 0))
 (defn count-inc! [] (swap! !count inc))

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

 (rcf/tests
  "normal heartbeat"

  (reset! !count 0)
  (def res-timeout-test-0
    (sci-run*
     {:code
      #_{:clj-kondo/ignore [:unresolved-namespace]}
      (with-sci-code
        (temporal.workflow/with-activity-options
          {:retryOptions {:maximumAttempts 2}
           :startToCloseTimeout [:sec 15]
           :heartbeatTimeout [:ms 200]}
          (temporal.workflow/with-sci-activity
            {:namespaces {'user {'count-inc! (str `count-inc!)
                                 'long-task-with-heartbeat (str `long-task-with-heartbeat)}}}
            (user/count-inc!)
            (user/long-task-with-heartbeat
             {:hb-interval 100 :task-ms 1000}))))}))
  res-timeout-test-0 := "done"
  @!count := 1)

 (rcf/tests
  "abnormal heartbeat"

  (reset! !count 0)
  (def res-timeout-test-1
    #_{:clj-kondo/ignore [:unresolved-namespace]}
    (try [:ok (sci-run*
               {:code
                (with-sci-code
                  (temporal.workflow/with-activity-options
                    {:retryOptions {:maximumAttempts 2}
                     :startToCloseTimeout [:sec 15]
                     :heartbeatTimeout [:ms 100]}
                    (temporal.workflow/with-sci-activity
                      {:namespaces {'user {'count-inc! (str `count-inc!)
                                           'long-task-with-heartbeat (str `long-task-with-heartbeat)}}}
                      (user/count-inc!)
                      (user/long-task-with-heartbeat
                      ;; 300ms > 100ms, will cause heartbeat timeout
                       {:hb-interval 300 :task-ms 2000}))))})]
         (catch Throwable t [:error t])))
  (first res-timeout-test-1) := :error
  @!count := 2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Activity
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(rcf/tests
 "call activity within workflow"

 (sci-run*
  {:code (with-sci-code
           (temporal.workflow/execute-activity
            "babashka/sci" {:code "(+ 4 2)"}))})
 := 6

 (defn add2 [a b] (+ a b))

 ;; with params & customized namespaces
 (sci-run*
  {:code (with-sci-code
           (temporal.workflow/execute-activity
            "babashka/sci"
            {:params 4
             :namespaces {'user {'add2 (:add2-name temporal.workflow/params)}}
             :code "(user/add2 temporal.activity/params 2)"}))
   ;; notice the runtime value should passed in via `params`
   :params {:add2-name (str `add2)}})
 := 6

 ;; a helper macro (witin workflow sci's evaluation environment)
 (sci-run*
  {:code
   (with-sci-code
     (temporal.workflow/with-sci-activity
       (+ 4 2)))})
 := 6

 (sci-run*
  {:code
   (with-sci-code
     (temporal.workflow/with-sci-activity
       {:params 4}
       (+ temporal.activity/params 2)))})
 := 6

 ;; async execution
 (sci-run*
  {:code
   (with-sci-code
     @(temporal.workflow/with-sci-activity-async
        {:params 4}
        (+ temporal.activity/params 2)))})
 := 6)

(binding [rcf/*timeout* 2000]
  (rcf/tests
   "async actually works"

   ;; won't take 10 * 1000ms
   (sci-run*
    {:code
     (with-sci-code
       (let [vs (for [i (range 10)]
                  (temporal.workflow/with-sci-activity-async
                        ;; runtime value needs to pass to activity code via params
                    {:params i}
                    (temporal.sci/sleep 1000)
                    temporal.activity/params))]
         (doall vs)
         (reduce (fn [r i] (+ r @i)) 0 vs)))})
   := 45))

(rcf/tests
 "activity exception defaults to be not retryable"

 (try (sci-run*
       {:code (with-sci-code
                (temporal.workflow/with-sci-activity
                  (throw (ex-info "" {}))))})
      (catch Throwable t (type t)))
 := io.temporal.client.WorkflowFailedException)

(binding [rcf/*timeout* 3000]
  (def !m (atom {}))
  (defn inc-v! [k] (swap! !m update k (fnil inc 0)))

  (rcf/tests
   "retryable exception causes activity retries forever"
   (def wf-id (str (random-uuid)))

   (future (try (sci-run*
                 {:namespaces {'user {'inc-v! (str `inc-v!)}}
                  :code (with-sci-code
                          (inc-v! :workflow)
                          (temporal.workflow/with-sci-activity
                            {:namespaces (:namespaces temporal.workflow/input)}
                            (inc-v! :activity)
                            (throw (ex-info "" {:temporal/retryable true}))))})
                (catch Throwable t (rcf/tap (type t)))))
   ;; it will retry forever, here is the check
   rcf/% := ::rcf/timeout
   (= (:workflow @!m) 1) := true
   (> (:activity @!m) 1) := true
   (terminate-workflow wf-id))

  (rcf/tests
   "setup retry options manually"
   (def wf-id (str (random-uuid)))
   (def !f (future (try (sci-run*
                         {:namespaces {'user {'inc-v! (str `inc-v!)}}
                          :code (with-sci-code
                                  (inc-v! :workflow)
                                  (temporal.workflow/with-activity-options
                                    [:retryOptions {:initialInterval [:ms 100]
                                                    :maximumAttempts 2}
                                     :startToCloseTimeout [:sec 60]]
                                    (temporal.workflow/with-sci-activity
                                      {:namespaces (:namespaces temporal.workflow/input)}
                                      (inc-v! :activity)
                                      (throw (ex-info "" {:temporal/retryable true})))))})
                        (catch Throwable t (rcf/tap (type t))))))
   ;; notice here the retry should stop on 2 attempts
   (= (:workflow @!m) 1) := true
   (= (:activity @!m) 2) := true
   (not= (deref !f 100 ::timeout) ::timeout) := true
   (when (= (deref !f 100 ::timeout) ::timeout)
     (terminate-workflow wf-id))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Workflow messaging
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(rcf/tests
 (def wf-id (str (random-uuid)))
 (def !n (atom 0))
 (defn inc-n! [n] (swap! !n + n))
 (def !f (future (sci-run* wf-id
                   {:code (with-sci-code
                            (temporal.workflow/wait-condition
                             (fn [] (:done? temporal.workflow/state)))
                            (:n temporal.workflow/state))
                    :namespaces {'user {'inc-n! (str `inc-n!)}}})))

 ;; wait for ready
 (Thread/sleep 600)

 (def stub (lotuc.temporal.sci/sci-workflow-stub (message-options wf-id)))

 (reset! !n 0)
 ;; action params can be retrieved from `temporal.workflow/action-params`
 (def update-code
   (with-sci-code
     (let [n temporal.workflow/action-params]
       (inc-n! n)
       (alter-var-root #'temporal.workflow/state update :n (fnil + 0) n))
     (:n temporal.workflow/state)))
 (def query-code
   (with-sci-code
     (:n temporal.workflow/state)))
 (def signal-done-code
   (with-sci-code
     (alter-var-root #'temporal.workflow/state assoc :done? true)))

 (lotuc.temporal.sci/update! stub {:code update-code :params 1}) := 1
 @!n = 1
 (lotuc.temporal.sci/query stub {:code query-code}) := 1

 (lotuc.temporal.sci/update-silence-on-abandom! stub {:code update-code :params 41}) := 42
 @!n = 42
 (lotuc.temporal.sci/query stub {:code query-code}) := 42

 (lotuc.temporal.sci/signal! stub {:code signal-done-code})
 @!f = 42

 ;; you can query when workflow is done, it will replay your code
 (reset! !n 0)
 (lotuc.temporal.sci/query stub {:code query-code}) := 42
 @!n = 42

 (reset! !n 0)
 (def query-code-with-parmas
   (with-sci-code
     (+ temporal.workflow/action-params (:n temporal.workflow/state))))
 (->> {:code query-code-with-parmas :params 24}
      (lotuc.temporal.sci/query stub))
 := 66

 (def !f (future (sci-run* wf-id
                   {:code (with-sci-code
                            (temporal.workflow/wait-condition
                             (fn [] (:done? temporal.workflow/state)))
                            (:n temporal.workflow/state))})))
 (Thread/sleep 600)
 ;; this signal function also works
 (lotuc.temporal.sci/signal-silence-on-abandom! stub {:code signal-done-code})
 @!f = nil
 #_())

(rcf/tests
 (def wf-id (str (random-uuid)))
 (def !f (future (sci-run* wf-id
                   {:code (with-sci-code
                            (temporal.workflow/wait-condition
                             (fn [] (:done? temporal.workflow/state)))
                            (:n temporal.workflow/state))})))

;; wait for ready
 (Thread/sleep 600)

 (def stub (lotuc.temporal.sci/sci-workflow-stub (message-options wf-id)))

 ;; update failure
 (try (->> {:code
            (with-sci-code
              (alter-var-root #'temporal.workflow/state update :n (fnil inc 0))
              (:n temporal.workflow/state))
            :validator-code
            (with-sci-code
              ;; can accept params
              ;; (prn temporal.workflow/action-params)
              (throw (ex-info "invalid update" {})))
            :params {:hello "world"}}
           (lotuc.temporal.sci/update! stub))
      (catch Throwable e (type e)))
 := io.temporal.client.WorkflowUpdateException

 ;; won't affect the workflow
 (deref !f 100 ::timeout) := ::timeout

 (try (->> {:code
            (with-sci-code
              (alter-var-root #'temporal.workflow/state assoc :done? true))}
           (lotuc.temporal.sci/signal! stub))
      (catch Throwable e (type e)))

 @!f = nil

 #_())

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; async functions

(binding [rcf/*timeout* 2000]
  (rcf/tests
   "async-call"

   (def n 8)
   (future
     (rcf/tap (sci-run*
               {:params n
                :code (with-sci-code
                        (let [tasks
                              (for [i (range temporal.workflow/params)]
                                (temporal.workflow/async-call-function
                                 (fn [v] (temporal.sci/sleep 500) v)
                                 i))]
                          (reduce (fn [r i] (+ r @i)) 0 tasks)))})))
   rcf/% := (reduce + (range n))))

(rcf/tests
 "async-call & promise ops"

 ;; promise-get
 (future
   (rcf/tap (sci-run*
             {:code
              (with-sci-code
                (-> (temporal.workflow/async-call-function (fn [] 42))
                    (temporal.workflow/promise-get)))})))
 rcf/% := 42

 ;; promise-then :handle (default operation)
 (rcf/tap (sci-run*
           {:code (with-sci-code
                    [(-> (temporal.workflow/async-call-function (fn [] 42))
                         (temporal.workflow/promise-then
                          :handle (fn [v _err] (+ v 24)))
                         (temporal.workflow/promise-get))
                     (-> (temporal.workflow/async-call-function (fn [] 42))
                         (temporal.workflow/promise-then
                          (fn [v _err] (+ v 24)))
                         (temporal.workflow/promise-get))])}))
 rcf/% := [66 66]

;; promise-then apply
 (rcf/tap (sci-run*
           {:code (with-sci-code
                    (-> (temporal.workflow/async-call-function (fn [] 42))
                        (temporal.workflow/promise-then
                         :apply (fn [v] (+ v 24)))
                        (temporal.workflow/promise-get)))}))
 rcf/% := 66

 ;; promise-then handle-exception
 (rcf/tap (sci-run*
           {:code (with-sci-code
                    (-> (temporal.workflow/async-call-function (fn [] (/ 1 0)))
                        (temporal.workflow/promise-then
                         :handle-exception (fn [_e] 66))
                        (temporal.workflow/promise-get)))}))
 rcf/% := 66

 ;; promise-then compose
 (rcf/tap
  (sci-run*
   {:code
    (with-sci-code
      (-> (temporal.workflow/async-call-function (fn [] 42))
          (temporal.workflow/promise-then
           :compose (fn [v]
                      (temporal.workflow/async-call-function
                       (fn [] (+ v 24)))))
          (temporal.workflow/promise-get)))}))
 rcf/% := 66)

(binding [rcf/*timeout* 4000]
  (rcf/tests
   "compose async activity results"
   (sci-run*
    {:params 10
     :code
     (with-sci-code
       (let [vs (for [i (range temporal.workflow/params)]
                  (let [options temporal.workflow/activity-options]
                    (temporal.workflow/async
                     (temporal.sci/sleep (rand-int 2000))
                     (temporal.workflow/with-activity-options
                       options
                       @(temporal.workflow/with-sci-activity-async
                          {:params i}
                          (temporal.sci/sleep (rand-int 1000))
                          temporal.activity/params)))))]
         (doall vs)
         @(reduce (fn [r i]
                    (temporal.workflow/promise-then
                     r :compose
                     (fn [rv]
                       (temporal.workflow/promise-then
                        i :compose
                        (fn [iv]
                          (temporal.workflow/async-call-function
                           (fn [] (+ rv iv))))))))
                  (temporal.workflow/async-call-function (fn [] 0))
                  vs)))})
   := 45))
