(ns lotuc.sample.temporal.sample07-sci
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.camel-snake-kebab.core :as csk]
   [lotuc.sample.temporal.common :as common]
   [lotuc.temporal.csk :as temporal.csk]
   [sci.core :as sci]
   [taoensso.telemere :as tel]))

(def clojure-core
  {'time
   ^:sci/macro
   (fn [_&form _&env expr]
     `(let [start# (clojure.core/now-nano-time)
            ret#   ~expr]
        (prn (str "Elapsed time: "
                  (/ (double (- (clojure.core/now-nano-time) start#))
                     1000000.0) " msecs"))
        ret#))
   'even? even?
   'odd? odd?
   'print print
   'prn prn
   'println println
   'ex-info ex-info
   'ex-data ex-data
   'ex-cause ex-cause
   'ex-message ex-message

   ;; -- for testing --
   'sleep (fn [v] (Thread/sleep v))
   'now-nano-time (fn [] (. System (nanoTime)))})

(definterface ^{io.temporal.workflow.WorkflowInterface []}
  SciWorkflow
  (^{io.temporal.workflow.WorkflowMethod []} run [_params])
  (^{io.temporal.workflow.QueryMethod []} getState [])
  (^{io.temporal.workflow.SignalMethod []} ^void pause [])
  (^{io.temporal.workflow.SignalMethod []} ^void resume []))

(definterface ^{io.temporal.activity.ActivityInterface []}
  SciActivity
  (^{io.temporal.activity.ActivityMethod {:name "babashka/sci"}} sci [v]))

(defrecord SciActivityImpl [ctx]
  SciActivity
  (sci [_ v] (sci/eval-string* ctx v)))

(defn async-call [f]
  (io.temporal.workflow.Async/function
   (reify io.temporal.workflow.Functions$Func
     (apply [_] (f)))))

(defmacro async [& body]
  `(auto-wf/async-call (^{:once true} fn* [] ~@body)))

(defonce !sci-workflow-state (atom {}))

(defn workflow-state-id
  ([] (workflow-state-id (io.temporal.workflow.Workflow/getInfo)))
  ([^io.temporal.workflow.WorkflowInfo info]
   [(.getNamespace info)
    (.getWorkflowType info)
    (.getWorkflowId info)
    (.getRunId info)]))

(comment
  (reset! !sci-workflow-state nil)
  #_())

(defn execute-activity
  [^io.temporal.workflow.ActivityStub stub activity-name & args]
  (prn :execute [activity-name args])
  (.execute stub activity-name Object (into-array Object args)))

(defn execute-async-activity
  [^io.temporal.workflow.ActivityStub stub activity-name & args]
  (.executeAsync stub activity-name Object (into-array Object args)))

(defn sci-ns
  [{:keys [activity-stub params !state !eval-state pause wait-pause]}]
  (letfn [(execute-activity* [activity-name & args]
            (wait-pause)
            (apply execute-activity activity-stub activity-name args))
          (execute-async-activity* [activity-name & args]
            (wait-pause)
            (apply execute-async-activity activity-stub activity-name args))
          (async-call* [f]
            (let [v (async-call f)]
              (swap! !eval-state update :!asyncs conj v)
              v))
          (await! [v]
            (when v (.get v)))
          (await-all! []
            (doseq [v (:!asyncs @!eval-state)]
              (try (await! v) (catch Throwable _))))
          (future-call [f]
            (let [fut (future-call f)]
              (swap! !eval-state update :!futures conj fut)
              fut))]
    {'temporal.activity
     {'params params
      '!state !state
      'pause pause
      'async-call async-call*
      'async #'async
      'await! await!
      'await-all! await-all!
      'execute execute-activity*
      'execute-async execute-async-activity*}
     'clojure.core
     (merge clojure-core
            {'future-call future-call
             'future  #'future})}))

(def task-queue (str ::queue))

(defn sci-run! [{:keys [wf-state-id pause code params]}]
  (let [activity-stub
        (io.temporal.workflow.Workflow/newUntypedActivityStub
         (-> (io.temporal.activity.ActivityOptions/newBuilder)
             (.setTaskQueue task-queue)
             (.setStartToCloseTimeout (java.time.Duration/ofMinutes 30))
             (.setHeartbeatTimeout (java.time.Duration/ofSeconds 10))
             (.setRetryOptions
              (-> (io.temporal.common.RetryOptions/newBuilder)
                  (.setMaximumAttempts 1)
                  (.build)))
             (.build)))

        wait-pause
        (fn []
          (when (get-in @!sci-workflow-state [wf-state-id :pause?])
            ;; triggers an event on UI
            ;; https://github.com/temporalio/ui/issues/1002
            (-> (io.temporal.workflow.Workflow/newTimer
                 (java.time.Duration/ofSeconds 1)
                 (-> (io.temporal.workflow.TimerOptions/newBuilder)
                     (.setSummary "wait-pause")
                     (.build)))
                (.get))
            (io.temporal.workflow.Workflow/await
             (fn [] (not (get-in @!sci-workflow-state [wf-state-id :pause?]))))))

        pause
        (fn [] (pause) (wait-pause))

        !state (atom nil)
        !eval-state (atom {})

        sci-namespaces
        (sci-ns
         {:activity-stub activity-stub

          :!eval-state !eval-state
          :wait-pause wait-pause

          ;; script state
          :params params
          :!state !state
          :pause pause})
        sci-ctx
        (sci/init {:namespaces sci-namespaces})]

    (swap! !sci-workflow-state update wf-state-id assoc
           :eval-state !eval-state
           :state !state
           :sci-ctx sci-ctx)

    (try (let [r (sci/eval-string* sci-ctx code)]
           (when-some [futures (:futures @!eval-state)]
             (let [n (count futures)]
               (doseq [[i fut] (map-indexed vector r)
                       :let [i (inc i)]]
                 (tel/log! {:level :info :msg "waiting futures" :data {:i i :n n}})
                 (try @fut (catch Throwable t
                             (tel/log! {:level :info
                                        :msg "failed future"
                                        :error t}))))))
           r)
         (finally
           (swap! !sci-workflow-state dissoc wf-state-id)))))

(defrecord SciWorkflowImpl []
  SciWorkflow
  (run [this params]
    (let [this-id (workflow-state-id)
          params (-> (temporal.csk/transform-keys csk/->kebab-case-keyword params)
                     (assoc :wf-state-id this-id :pause #(.pause this)))]
      (->> (sci-run! params)
           (temporal.csk/transform-keys csk/->kebab-case-string))))
  (getState [_]
    (let [this-id (workflow-state-id)]
      (some-> (get @!sci-workflow-state this-id) (:state) deref)))
  (pause [_]
    (let [this-id (workflow-state-id)]
      (swap! !sci-workflow-state assoc-in [this-id :pause?] true)))
  (resume [_]
    (let [this-id (workflow-state-id)]
      (swap! !sci-workflow-state assoc-in [this-id :pause?] false))))

(defn wf-options [workflow-id]
  (-> (io.temporal.client.WorkflowOptions/newBuilder)
      (.setWorkflowId workflow-id)
      (.setTaskQueue task-queue)
      (.build)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(ig.repl/set-prep!
 #(ig/expand
   {:sample/default-client {}
    :sample/worker
    {:client (ig/ref :sample/default-client)
     :task-queue task-queue
     :workflow-classes [SciWorkflowImpl]
     :activity-instances
     [(map->SciActivityImpl {:ctx (sci/init {:namespaces {'clojure.core clojure-core}})})]}}))

(defn create-&-run-workflow
  [workflow-id params]
  (future
    (-> (common/default-client)
        (.newWorkflowStub SciWorkflow (wf-options workflow-id))
        (.run (temporal.csk/transform-keys csk/->kebab-case-string params)))))

(comment

  (bean
   (-> (.newUntypedWorkflowStub
        (common/default-client)
        "39c46dfc-ff2c-492b-8c48-06d6849584b4")
       (.describe)
       ;; (.getWorkflowExecutionInfo)
       ;; (.getExecution)
       ;; (.getStatus)
       ;; (.getDescription)
       )
   #_())

  (-> (.newUntypedWorkflowStub
       (common/default-client)
       "39c46dfc-ff2c-492b-8c48-06d6849584b4")
      ;; (.getExecution)
      ;; (.getOptions)
      ;; bean
      )

  io.temporal.client.WorkflowClient
  io.temporal.client.WorkflowStub
  io.temporal.client.WorkflowExecutionDescription
  io.temporal.client.WorkflowExecutionMetadata
  io.temporal.client.WorkflowExecution
  io.temporal.api.workflow.v1.WorkflowExecutionInfo

  (type (common/default-client))

  @(create-&-run-workflow "wf00" {:params [] :code "(+ 4 2)"})

  @(->> {:params [] :code "(try (temporal.activity/execute \"babashka/sci\" \"(/ 4 0)\")
                                (catch io.temporal.failure.ActivityFailure e

            (println)
            (println (type e) (ex-data e) (ex-message e) )
            (println)
            (let [e (ex-cause e)]
              (println :> (type e) (ex-data e) (ex-message e) )

              (let [e (ex-cause e)]
                (println :> (type e) (ex-data e) (ex-message e) ))

            )
        ))"}
        (create-&-run-workflow "wf00"))

  (def r
    (->> {:params []
          :code "
  (require '[temporal.activity :refer :all])
  (let [xs (for [i (range 100)]
             (if (odd? i)
               (execute-async \"babashka/sci\" \"(do (sleep 3000) 24)\")
               (execute-async \"babashka/sci\" \"(do (sleep 1500) 24)\") ))
        _ (println \"001>>\")
        r (time (reduce (fn [r i] (+ r (await! i))) 0 xs))]
    (println \"002>>\")
    (time (pause))
    r)"}
         (create-&-run-workflow "wf01")))
  (.resume (.newWorkflowStub (common/default-client) SciWorkflow "wf01"))

  #_())
