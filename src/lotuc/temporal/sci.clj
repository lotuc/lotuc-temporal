(ns lotuc.temporal.sci
  (:require
   [clojure.java.data :as j]
   [clojure.java.io :as io]
   [lotuc.sci-rt.temporal.activity :as temporal.activity]
   [lotuc.sci-rt.temporal.csk :as temporal.csk]
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

(declare wrap-call1 wrap-call2)

(defonce !sci-activity-preset-namespaces (atom nil))

(defonce !sci-workflow-preset-namespaces (atom nil))

(defn alter-preset-namespaces! [rt f & args]
  (let [as (case rt
             :workflow [!sci-workflow-preset-namespaces]
             :activity [!sci-activity-preset-namespaces]
             :all      [!sci-workflow-preset-namespaces !sci-activity-preset-namespaces])]
    (doseq [!a as]
      (apply swap! !a f args))))

(defn load-preset-namespaces [rt shared-vars namespace-spec]
  (let [preset (case rt
                 :workflow @!sci-workflow-preset-namespaces
                 :activity @!sci-activity-preset-namespaces)

        wrap-fn (case rt
                  :workflow
                  #(binding [temporal.workflow/sci-vars shared-vars]
                     (temporal.workflow/wrap-fn-with-shared-dynamic-vars-copied-out-sci %))
                  :activity
                  #(binding [temporal.activity/sci-vars shared-vars]
                     (temporal.activity/wrap-fn-with-shared-dynamic-vars-copied-out-sci %)))

        ns-loader
        (letfn [(wrap-var [v]
                  (if (let [m (meta v)] (or (:sci/macro m) (:macro m)))
                    v
                    (wrap-fn v)))]
          (memoize
           (fn ns-loader [ns-sym]
             (let [ns-vars (get preset ns-sym)]
               (when-not ns-vars
                 (throw (temporal.ex/ex-info-do-not-retry
                         "UNSUPPORTED_PRESET_NAMESPACE"
                         {:namespace ns-sym})))
               (memoize
                (fn ns-var-loader
                  ([]
                   (reduce-kv (fn [m k v] (assoc m k (wrap-var v)))
                              {}
                              (if (fn? ns-vars) (ns-vars) ns-vars)))
                  ([ns-var-sym]
                   (if-some [v (if (fn? ns-vars)
                                 (ns-vars ns-var-sym)
                                 (get ns-vars ns-var-sym))]
                     (wrap-var v)
                     (throw (temporal.ex/ex-info-do-not-retry
                             "UNSUPPORTED_PRESET_NAMESPACE_VAR"
                             {:namespace ns-sym :var ns-var-sym}))))))))))
        ns-load-vars
        (fn ns-load [ns-loader-fn ns-vars]
          (cond
            (coll? ns-vars)
            (reduce (fn [m var-sym] (assoc m var-sym (ns-loader-fn var-sym)))
                    {}
                    (map symbol ns-vars))

            (= (symbol ns-vars) 'all)
            (ns-loader-fn)

            :else
            (recur ns-loader-fn [ns-vars])))]

    (try
      (reduce-kv (fn [m k v] (let [ns-sym (symbol k)]
                               (assoc m ns-sym (ns-load-vars (ns-loader ns-sym) v))))
                 {}
                 namespace-spec)
      (catch Exception t
        (if (temporal.ex/ex-with-retryable-signature? t)
          (throw t)
          (throw (temporal.ex/ex-info-do-not-retry
                  "invalid namespace spec" {:namespace-spec namespace-spec})))))))

(defn build-sci-activity-ns [vars namespace-spec]
  (encore/nested-merge
   (load-preset-namespaces :activity vars namespace-spec)
   (temporal.sci/sci-default-namespaces :activity)
   {'lotuc.sci-rt.temporal.activity
    (merge
     ;; vars
     vars
     ;; fns
     (temporal.activity/sci-fns vars))}))

(defn build-sci-workflow-ns
  ([vars namespace-spec] (build-sci-workflow-ns vars namespace-spec nil))
  ([vars namespace-spec random]
   (let [random (or random (io.temporal.workflow.Workflow/newRandom))]
     (encore/nested-merge
      (load-preset-namespaces :workflow vars namespace-spec)
      (temporal.sci/sci-default-namespaces :workflow random)
      {'lotuc.sci-rt.temporal.workflow
       (merge
        ;; vars
        vars
        ;; fns
        (temporal.workflow/sci-fns vars)
        ;; macros
        temporal.workflow/sci-macros)}))))

(defn string-sha256* [string]
  (let [digest (.digest (java.security.MessageDigest/getInstance "SHA-256")
                        (String/.getBytes string "UTF-8"))]
    (apply str (map (partial format "%02x") digest))))

(defn load-sci-code [code]
  (if (string? code)
    code
    (let [{:keys [code-path sha256]} code]
      (when (not code-path)
        (throw (temporal.ex/ex-info-do-not-retry "invalid sci code" {:code code})))
      (when (not sha256)
        (throw (temporal.ex/ex-info-do-not-retry "sha256 is required for code-path" {:code code})))
      (let [code (try (or (some-> (io/resource code-path) (slurp))
                          (slurp (io/file code-path)))
                      (catch Throwable _
                        (throw (temporal.ex/ex-info-do-not-retry
                                "failed loading code" {:code code}))))]
        (when-not (= (string-sha256* code) sha256)
          (throw (temporal.ex/ex-info-do-not-retry
                  "sha256 check failed" {:code code})))
        code))))

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

(defn activity-run* [opts {:keys [code params retryable-code namespaces] :as input}]
  (let [vars (temporal.activity/new-sci-vars)
        opts' (encore/nested-merge
               {:namespaces (build-sci-activity-ns vars namespaces)
                :ns-aliases temporal.sci/sci-ns-aliases}
               opts)
        ctx (sci/init opts')
        ctx-readonly (delay (sci/init (assoc opts' :deny ['alter-var-root])))]
    (letfn [(retryable? [t]
              (if-some [retryable-code (some-> retryable-code load-sci-code)]
                (try (binding [temporal.activity/sci-vars vars]
                       (temporal.activity/with-shared-dynamic-vars-bound-to-sci-vars*
                         {'input input
                          'params params}
                         ((sci/eval-string* @ctx-readonly retryable-code) t)))
                     (catch Throwable _e1 false))
                false))]
      (try (binding [temporal.activity/sci-vars vars]
             (temporal.activity/with-shared-dynamic-vars-bound-to-sci-vars*
               {'input input
                'params params}
               (sci/eval-string* ctx (load-sci-code code))))
           (catch Throwable t
             (temporal.ex/rethrow-toplevel t retryable?))))))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]}
(defrecord SciActivityImpl [opts]
  SciActivity
  (sci [_ params]
    (wrap-call1 activity-run* opts params)))

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
  ([{:keys [ctx ctx-readonly vars input] :as _state-map}
    {:keys [action code retryable-code]}]
   (let [params (:params input)
         activity-options
         (or (:activity-options params)
             {:startToCloseTimeout [:sec 60]})]
     (letfn [(retryable? [t]
               (if-some [retryable-code (some-> retryable-code load-sci-code)]
                 (try (binding [temporal.workflow/sci-vars vars]
                        (temporal.workflow/with-shared-dynamic-vars-bound-to-sci-vars*
                          {'input input
                           'params params
                           'activity-options activity-options}
                          ((sci/eval-string retryable-code ctx-readonly)
                           t)))
                      (catch Throwable _e false))
                 false))]
       (try
         (binding [temporal.workflow/sci-vars vars]
           (temporal.workflow/with-shared-dynamic-vars-bound-to-sci-vars*
             {'input input
              'params params
              'activity-options activity-options
              'action-input (:input action)
              'action-name (:name action)
              'action-params (get-in action [:input :params])}
             ;; use `ctx` by default, fallback to readonly one.
             (let [r (sci/eval-string* (or ctx ctx-readonly) (load-sci-code code))]
               (if-some [f (when (not action)
                             (if (fn? r) r
                                 (when (instance? sci.lang.Var r)
                                   (when (fn? @r) @r))))]
                 (f input)
                 r))))
         (catch Throwable t
           (temporal.ex/rethrow-toplevel t (or retryable? (constantly false)))))))))

(defn workflow-run* [this {:keys [code retryable-code state namespaces] :as input}]
  (let [vars (temporal.workflow/new-sci-vars)]
    (when state (sci/alter-var-root ('state vars) (constantly state)))
    (try
      (let [wf-state-id      (wf-run-id)
            _                (->> {:track-type :gc
                                   :dispose-fn #(swap! !wf-run-state dissoc wf-state-id)}
                                  (resource/track this))
            opts             (encore/nested-merge
                              {:namespaces (build-sci-workflow-ns vars namespaces)
                               :ns-aliases temporal.sci/sci-ns-aliases})
            ctx              (sci/init opts)
            ctx-readonly     (sci/init (assoc opts :deny ['alter-var-root]))
            state-map        {:input input
                              :vars vars
                              :ctx ctx
                              :ctx-readonly ctx-readonly}]
        (swap! !wf-run-state assoc wf-state-id state-map)
        (workflow-run-sci-code! state-map {:code code :retryable-code retryable-code}))
      (catch Throwable t
        (let [d (ex-data t)]
          (if (:temporal/continue-as-new d)
            (do (io.temporal.workflow.Workflow/continueAsNew
                 (into-array Object [(temporal.csk/transform-named->string
                                      (merge
                                       (assoc input :state @('state vars))
                                       (select-keys d [:params])))]))
                nil)
            (temporal.ex/rethrow-toplevel t)))))))

(defn query* [{:keys [code] :as p}]
  (let [state-map (-> (get @!wf-run-state (wf-run-id))
                      ;; *exclude* writable ctx.
                      (dissoc :ctx))]
    (workflow-run-sci-code!
     state-map {:action {:name :query :input p}
                :code   code})))

(defn update* [{:keys [code] :as p}]
  (let [state-map (get @!wf-run-state (wf-run-id))]
    (workflow-run-sci-code!
     state-map {:action {:name :update :input p}
                :code   code})))

(defn update-validator* [{:keys [validator-code] :as p}]
  (when validator-code
    (let [state-map (get @!wf-run-state (wf-run-id))]
      (workflow-run-sci-code!
       state-map {:action {:name :update-validator :input p}
                  :code   validator-code}))))

(defn signal* [{:keys [code] :as p}]
  (let [state-map (get @!wf-run-state (wf-run-id))]
    (workflow-run-sci-code!
     state-map {:action {:name :signal :input p}
                :code   code})))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]}
(defrecord SciWorkflowImpl []
  SciWorkflow
  (sciRun [this params]
    (wrap-call1 workflow-run* this params))
  (handleQuery [_ params]
    (wrap-call1 query* params))
  (handleUpdate [_ params]
    (wrap-call1 update* params))
  (handleUpdateValidator [_ params]
    (wrap-call1 update-validator* params))
  (handleUpdateSilenceOnAbandon [_ params]
    (wrap-call1 signal* params))
  (handleUpdateSilenceOnAbandonValidator [_ params]
    (wrap-call1 update-validator* params))
  (handleSignal [_ params]
    (wrap-call1 signal* params))
  (handleSignalSilenceOnAbandon [_ params]
    (wrap-call1 signal* params)))

(defn ^{:arglists '([^SciWorkflow wf {:keys [code retryable-code namespaces]}])
        :style/indent 1}
  sci-run!
  [^SciWorkflow wf p]
  (->> (temporal.csk/transform-named->string p)
       (.sciRun wf)
       (temporal.csk/transform-keys->keyword)))

(defn query [^SciWorkflow wf p]
  (wrap-call2 lotuc.temporal.sci.SciWorkflow/.handleQuery wf p))

(defn ^{:arglists '([^SciWorkflow wf {:keys [code validator-code]}])}
  signal!
  [^SciWorkflow wf p]
  (wrap-call2 lotuc.temporal.sci.SciWorkflow/.handleSignal wf p))

(defn ^{:arglists '([^SciWorkflow wf {:keys [code validator-code]}])}
  signal-silence-on-abandom!
  [^SciWorkflow wf p]
  (wrap-call2 lotuc.temporal.sci.SciWorkflow/.handleSignalSilenceOnAbandon wf p))

(defn ^{:arglists '([^SciWorkflow wf {:keys [code validator-code]}])}
  update!
  [^SciWorkflow wf p]
  (wrap-call2 lotuc.temporal.sci.SciWorkflow/.handleUpdate wf p))

(defn ^{:arglists '([^SciWorkflow wf {:keys [code validator-code]}])}
  update-silence-on-abandom!
  [^SciWorkflow wf p]
  (wrap-call2 lotuc.temporal.sci.SciWorkflow/.handleUpdateSilenceOnAbandon wf p))

(defn ^{:doc "
  workflow-stub <- workflow-client <-
      [workflow-id | workflow-options] + workflow-client-options"}

  sci-workflow-stub
  [{:keys [^io.temporal.serviceclient.WorkflowServiceStubs
           workflow-stub
           ^io.temporal.client.WorkflowClient
           workflow-client

           workflow-service-stubs
           workflow-client-options
           workflow-id
           workflow-options]
    :as opts}]
  (cond
    workflow-stub workflow-stub
    workflow-client
    (if workflow-id
      (.newWorkflowStub workflow-client lotuc.temporal.sci.SciWorkflow ^String workflow-id)
      (let [^io.temporal.client.WorkflowOptions
            workflow-options (j/to-java io.temporal.client.WorkflowOptions workflow-options)]
        (.newWorkflowStub workflow-client lotuc.temporal.sci.SciWorkflow workflow-options)))
    :else
    (let [client
          (if workflow-client-options
            (->> (j/to-java io.temporal.client.WorkflowClientOptions workflow-client-options)
                 (io.temporal.client.WorkflowClient/newInstance workflow-service-stubs))
            (io.temporal.client.WorkflowClient/newInstance workflow-service-stubs))]
      (recur (assoc opts :workflow-client client)))))

(defn ^{:doc "Loadable by `load-sci-code`."}
  code-path->sci-code
  [code-path]
  {:code-path code-path
   :sha256 (string-sha256*
            (or (some-> (io/resource code-path) (slurp))
                (slurp (io/file code-path))))})

(defmacro with-sci-code
  [& code-form]
  (if (= (count code-form) 1)
    (pr-str (first code-form))
    (pr-str `(do ~@code-form))))

;;; Assuming all inputs on wire being serialized to JSON. When deserialize for
;;; Clojure/SCI use, convert keys into `keyword`, when about to return to wire,
;;; convert named (not only keys) to string.

;;; The serialization logic can be enhanced & make customizable later. Now it
;;; suits my own use cases.

(defn wrap-call1 [f & args]
  (->> (map temporal.csk/transform-keys->keyword args)
       (apply f)
       (temporal.csk/transform-named->string)))

(defn wrap-call2 [f & args]
  (->> (map temporal.csk/transform-named->string args)
       (apply f)
       (temporal.csk/transform-keys->keyword)))
