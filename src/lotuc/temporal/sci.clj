(ns lotuc.temporal.sci
  (:require
   [clojure.java.data :as j]
   [lotuc.sci-rt.temporal.activity]
   [lotuc.sci-rt.temporal.csk :as temporal.csk]
   [lotuc.sci-rt.temporal.env :as rt.temporal.env]
   [lotuc.sci-rt.temporal.ex :as temporal.ex]
   [lotuc.sci-rt.temporal.sci :as temporal.sci]
   [lotuc.sci-rt.temporal.workflow :as temporal.workflow]
   [lotuc.temporal.to-java]
   [sci.core :as sci]
   [taoensso.encore :as encore]
   [tech.v3.resource :as resource]))

(defn wf-run-id
  ([] (wf-run-id (io.temporal.workflow.Workflow/getInfo)))
  ([^io.temporal.workflow.WorkflowInfo info]
   [(.getNamespace info) (.getWorkflowType info)
    (.getWorkflowId info) (.getRunId info)]))

(defn clojure-core-deref
  ([ref]
   (if (instance? io.temporal.workflow.Promise ref)
     (.get ref)
     (clojure.core/deref ref)))
  ([ref timeout-ms timeout-val]
   (if (instance? io.temporal.workflow.Promise ref)
     (try (.get ref (java.time.Duration/ofMillis timeout-ms))
          (catch java.util.concurrent.TimeoutException _ timeout-val))
     (clojure.core/deref ref timeout-ms timeout-val))))

(def clojure-core-time
  ^:sci/macro
  (fn [_&form _&env expr]
    `(let [start# (temporal.sci/systemTimeMillis)
           ret#   ~expr]
       (prn (str "Elapsed time: "
                 (double (- (temporal.sci/systemTimeMillis) start#))
                 " msecs"))
       ret#)))

(def sci-ns-aliases
  {'temporal.activity 'lotuc.sci-rt.temporal.activity
   'temporal.workflow 'lotuc.sci-rt.temporal.workflow
   'temporal.csk      'lotuc.sci-rt.temporal.csk
   'temporal.sci      'lotuc.sci-rt.temporal.sci})

(def sci-ns
  {'lotuc.sci-rt.temporal.csk
   {'transform-keys temporal.csk/transform-keys
    '->string temporal.csk/->string
    '->keyword temporal.csk/->keyword}
   'lotuc.sci-rt.temporal.ex
   {'ex-info-retryable temporal.ex/ex-info-retryable
    'ex-info-do-not-retry temporal.ex/ex-info-do-not-retry}
   'lotuc.sci-rt.temporal.sci
   {'sleep Thread/sleep
    'systemTimeMillis System/currentTimeMillis}
   'clojure.core
   {'time clojure-core-time
    'deref clojure-core-deref
    'even? even?
    'odd? odd?
    'print print
    'prn prn
    'println println
    'ex-info ex-info
    'ex-data ex-data
    'ex-cause ex-cause
    'ex-message ex-message
    'random-uuid random-uuid
    'rand-int rand-int
    'rand rand}})

(defn build-sci-activity-ns [params]
  (let [^io.temporal.activity.ActivityExecutionContext ctx
        (io.temporal.activity.Activity/getExecutionContext)]
    (letfn [(heartbeat [v]
              (.heartbeat ctx v))]
      (encore/nested-merge
       sci-ns
       {'lotuc.sci-rt.temporal.activity
        {'execution-ctx ctx
         'heartbeat heartbeat
         'params params}}))))

(defn build-sci-workflow-ns
  [{:keys [params vars]}]
  (let [{:keys [state action-name action-params activity-options]} vars
        random (io.temporal.workflow.Workflow/newRandom)]
    (letfn [(build-activity-stub []
              (io.temporal.workflow.Workflow/newUntypedActivityStub
               (j/to-java io.temporal.activity.ActivityOptions @activity-options)))]
      (encore/nested-merge
       sci-ns
       {'clojure.core
        {'random-uuid io.temporal.workflow.Workflow/randomUUID
         'rand-int #(.nextInt random %)
         'rand (fn
                 ([] (.nextFloat random))
                 ([n] (* n (.nextFloat random))))}

        'lotuc.sci-rt.temporal.sci
        {'sleep io.temporal.workflow.Workflow/sleep
         'systemTimeMillis io.temporal.workflow.Workflow/currentTimeMillis}

        'lotuc.sci-rt.temporal.workflow
        {'params                  params
         'state                   state
         'action-name             action-name
         'action-params           action-params
         'async-run               temporal.workflow/sci-async-run
         'new-cancellation-scope  temporal.workflow/sci-new-cancellation-scope

         'activity-options        activity-options
         'with-activity-options   temporal.workflow/sci-with-activity-options

         'with-sci-activity       (temporal.workflow/sci-with-sci-activity
                                   'lotuc.sci-rt.temporal.workflow/execute-activity)
         'with-sci-activity-async (temporal.workflow/sci-with-sci-activity
                                   'lotuc.sci-rt.temporal.workflow/execute-activity-async)
         'execute-activity        (temporal.workflow/sci-execute-activity
                                   build-activity-stub)
         'execute-activity-async  (temporal.workflow/sci-execute-activity-async
                                   build-activity-stub)

         'with-retry              temporal.workflow/sci-with-retry
         'with-retry-async        temporal.workflow/sci-with-retry-async
         'retry                   temporal.workflow/retry
         'retry-async             temporal.workflow/retry-async}}))))

(definterface ^{io.temporal.activity.ActivityInterface []}
  SciActivity
  (^{io.temporal.activity.ActivityMethod {:name "babashka/sci"}} sci [v]))

(definterface ^{io.temporal.workflow.WorkflowInterface []}
  SciWorkflow
  (^{io.temporal.workflow.WorkflowMethod []} sciRun [_params])
  (^{io.temporal.workflow.QueryMethod []} handleQuery [_params])
  (^{io.temporal.workflow.UpdateMethod []} handleUpdate [_params])
  (^{io.temporal.workflow.UpdateMethod
     {:unfinishedPolicy io.temporal.workflow.HandlerUnfinishedPolicy/ABANDON}}
   handleUpdateSilenceOnAbandon [_params])
  (^{io.temporal.workflow.UpdateValidatorMethod {:updateName "handleUpdate"}}
   ^void handleUpdateValidator [_params])
  (^{io.temporal.workflow.UpdateValidatorMethod {:updateName "handleUpdateSilenceOnAbandon"}}
   ^void handleUpdateSilenceOnAbandonValidator [_params])
  (^{io.temporal.workflow.SignalMethod []} ^void handleSignal [_params])
  (^{io.temporal.workflow.SignalMethod
     {:unfinishedPolicy io.temporal.workflow.HandlerUnfinishedPolicy/ABANDON}}
   ^void handleSignalSilenceOnAbandon [_params]))

(defn activity-run* [opts {:keys [code params retryable-code namespaces]}]
  (let [opts' (encore/nested-merge
               {:namespaces (build-sci-activity-ns params)
                :ns-aliases sci-ns-aliases}
               opts
               {:namespaces
                (into {} (for [[ns-k ns-vars] namespaces]
                           [(symbol ns-k)
                            (into {} (for [[var-k var-ref] ns-vars]
                                       [(symbol var-k)
                                        (resolve (symbol var-ref))]))]))})
        ctx (sci/init opts')]
    (letfn [(retryable? [t]
              (if retryable-code
                (try (rt.temporal.env/with-env-temporal-activity {:params params}
                       ((sci/eval-string retryable-code
                                         {:namespaces sci-ns
                                          :deny ['alter-var-root]
                                          :ns-aliases sci-ns-aliases})
                        t))
                     (catch Throwable _e1 false))
                false))]
      (try (rt.temporal.env/with-env-temporal-activity {:params params}
             (sci/eval-string* ctx code))
           (catch Throwable t
             (temporal.ex/rethrow-toplevel t retryable?))))))

(defrecord SciActivityImpl [opts]
  SciActivity
  (sci [_ params]
    (->> (temporal.csk/transform-keys temporal.csk/->keyword params)
         (activity-run* (update opts :namespaces (fn [v] (if (fn? v) (v) v))))
         (temporal.csk/transform-keys temporal.csk/->string))))

;;; A class with stateful field will make things easier.
;;;
;;; But we're using `defrecord` for interface implementation, now we just use a
;;; external var for state saving & using `tech.v3.resource/track` for the gc of
;;; the external state.
;;;
;;; We cannot cleanup the external state when `WorkflowMethod` finishes running.
;;; When you run query methods on a finished workflow, temporal will replay the
;;; `WorkflowMethod` to the end, and then retrieve the final state.
(defonce !wf-run-state (atom {}))

(defn workflow-run-sci-code!
  ([{:keys [ctx params ctx-readonly vars]}
    {:keys [action code retryable?]}]
   (try
     (let [{:keys [action-name action-params activity-options]} vars]
       (rt.temporal.env/with-env-temporal-workflow
         {:params params
          :action-name (:action-name action)
          :action-params (:action-params action)
          :activity-options @activity-options}
         (sci/binding [action-name (:action-name action)
                       action-params (:action-params action)]
            ;; use `ctx` by default, fallback to readonly one.
           (sci/eval-string* (or ctx ctx-readonly) code))))
     (catch Throwable t
       (temporal.ex/rethrow-toplevel t (or retryable? (constantly false)))))))

(defn workflow-run* [this {:keys [code retryable-code namespaces] :as params}]
  (let [wf-state-id      (wf-run-id)
        _                (->> {:track-type :gc
                               :dispose-fn #(swap! !wf-run-state dissoc wf-state-id)}
                              (resource/track this))

        state            (sci/new-dynamic-var 'state nil)
        action-name      (sci/new-dynamic-var 'action-name nil)
        action-params    (sci/new-dynamic-var 'action-params nil)
        activity-options (->> (or (:activity-options params) {:startToCloseTimeout [:sec 60]})
                              (sci/new-dynamic-var 'activity-options))
        vars             {:state state
                          :action-name action-name
                          :action-params action-params
                          :activity-options activity-options}

        opts             (encore/nested-merge
                          {:namespaces (build-sci-workflow-ns {:params params :vars vars})
                           :ns-aliases sci-ns-aliases}
                          {:namespaces
                           (into {} (for [[ns-k ns-vars] namespaces]
                                      [(symbol ns-k)
                                       (into {} (for [[var-k var-ref] ns-vars]
                                                  [(symbol var-k)
                                                   (resolve (symbol var-ref))]))]))})
        ctx              (sci/init opts)
        ctx-readonly     (sci/init (assoc opts :deny ['alter-var-root]))
        state-map        {:params params
                          :ctx ctx
                          :ctx-readonly ctx-readonly
                          :vars vars}]
    (swap! !wf-run-state assoc wf-state-id state-map)
    (letfn [(retryable? [t]
              (if retryable-code
                (try (rt.temporal.env/with-env-temporal-workflow {:params params}
                       ((sci/eval-string retryable-code ctx-readonly)
                        t))
                     (catch Throwable _e false))
                false))]
      (workflow-run-sci-code! state-map {:code code :retryable? retryable?}))))

(defn query* [{:keys [code] :as p}]
  (let [state-map (-> (get @!wf-run-state (wf-run-id))
                      ;; *exclude* writable ctx.
                      (dissoc :ctx))]
    (workflow-run-sci-code!
     state-map {:action {:action-name :query :action-params p}
                :code   code})))

(defn update* [{:keys [code] :as p}]
  (let [state-map (get @!wf-run-state (wf-run-id))]
    (workflow-run-sci-code!
     state-map {:action {:action-name :update :action-params p}
                :code   code})))

(defn update-validator* [{:keys [validator-code] :as p}]
  (when validator-code
    (let [state-map (get @!wf-run-state (wf-run-id))]
      (workflow-run-sci-code!
       state-map {:action {:action-name :update-validator :action-params p}
                  :code   validator-code}))))

(defn signal* [{:keys [code] :as p}]
  (let [state-map (get @!wf-run-state (wf-run-id))]
    (workflow-run-sci-code!
     state-map {:action {:action-name :signal :action-params p}
                :code   code})))

(defrecord SciWorkflowImpl []
  SciWorkflow
  (sciRun [this params]
    ((temporal.csk/wrap-fn workflow-run*) this params))
  (handleQuery [_ params]
    ((temporal.csk/wrap-fn query*) params))
  (handleUpdate [_ params]
    ((temporal.csk/wrap-fn update*) params))
  (handleUpdateValidator [_ params]
    ((temporal.csk/wrap-fn update-validator*) params))
  (handleUpdateSilenceOnAbandon [_ params]
    ((temporal.csk/wrap-fn signal*) params))
  (handleUpdateSilenceOnAbandonValidator [_ params]
    ((temporal.csk/wrap-fn update-validator*) params))
  (handleSignal [_ params]
    ((temporal.csk/wrap-fn signal*) params))
  (handleSignalSilenceOnAbandon [_ params]
    (temporal.csk/wrap-fn (signal* params))))

(defn ^{:arglists '([^SciWorkflow wf {:keys [code retryable-code namespaces]}])}
  sci-run!
  [^SciWorkflow wf p]
  (->> (temporal.csk/transform-keys temporal.csk/->string p)
       (.sciRun wf)
       (temporal.csk/transform-keys keyword)))

(defn query [^SciWorkflow wf {:keys [code]}]
  (->> (.handleQuery wf {"code" code})
       (temporal.csk/transform-keys keyword)))

(defn signal! [^SciWorkflow wf {:keys [code validator-code]}]
  (->> {"code" code "validator-code" validator-code}
       (.handleSignal wf)))

(defn signal-silence-on-abandom!
  [^SciWorkflow wf {:keys [code validator-code]}]
  (->> {"code" code "validator-code" validator-code}
       (.handleSignalSilenceOnAbandonValidator wf)))

(defn update! [^SciWorkflow wf {:keys [code validator-code]}]
  (->> {"code" code "validator-code" validator-code}
       (.handleUpdate wf)
       (temporal.csk/transform-keys keyword)))

(defn update-silence-on-abandom!
  [^SciWorkflow wf {:keys [code validator-code]}]
  (->> {"code" code "validator-code" validator-code}
       (.handleUpdateSilenceOnAbandonValidator wf)
       (temporal.csk/transform-keys keyword)))

(defmacro with-sci-wf
  {:style/indent 1 :clj-kondo/lint-as 'clojure.core/let}
  [[action options] & body]
  (let [[validator-code retryable-code namespaces code]
        (let [code0 (first body)]
          (if (map? code0)
            [(pr-str (:validator code0))
             (pr-str (:retryable code0))
             (:namespaces code0)
             (pr-str `(do ~@(rest body)))]
            [nil nil nil (pr-str `(do ~@body))]))

        f
        (case action
          (_query query)
          `lotuc.temporal.sci/query
          (_signal! signal!)
          `lotuc.temporal.sci/signal!
          (_signal-silence-on-abandom!
           signal-silence-on-abandom!)
          `lotuc.temporal.sci/signal-silence-on-abandom!
          (_update! update!)
          `lotuc.temporal.sci/update!
          (_update-silence-on-abandom! update-silence-on-abandom!)
          `lotuc.temporal.sci/update-silence-on-abandom!
          (_ _sci-run! sci-run!)
          `lotuc.temporal.sci/sci-run!)]

    `(let [~action ~f

           ^io.temporal.serviceclient.WorkflowServiceStubs
           stubs#
           (:workflow-service-stubs ~options)

           client#
           (io.temporal.client.WorkflowClient/newInstance stubs#)

           stub#
           (if-some [^String workflow-id# (:workflow-id ~options)]
             (.newWorkflowStub client# lotuc.temporal.sci.SciWorkflow
                               ^String workflow-id#)
             (let [^io.temporal.client.WorkflowOptions
                   workflow-options#
                   (j/to-java io.temporal.client.WorkflowOptions (:workflow-options ~options))]
               (.newWorkflowStub client# lotuc.temporal.sci.SciWorkflow
                                 ^io.temporal.client.WorkflowOptions workflow-options#)))

           params#
           (cond-> {:code ~code}
             ~validator-code (assoc :validator-code ~validator-code)
             ~retryable-code (assoc :retryable-code ~retryable-code)
             ~namespaces (assoc :namespaces ~namespaces))]
       (~action stub# params#))))
