(ns lotuc.sci-rt.temporal.workflow
  (:require
   [clojure.java.data :as j]
   [lotuc.sci-rt.temporal.csk :as temporal.csk]
   [lotuc.sci-rt.temporal.ex :as temporal.ex]
   [lotuc.sci-rt.temporal.sci :as temporal.sci]
   [lotuc.sci-rt.temporal.workflow.async :as temmporal.workflow.async]
   [lotuc.sci-rt.temporal.workflow.promise :as temporal.workflow.promise]
   [sci.core :as sci]))

(defmacro var-syms []
  `['~'input
    '~'params
    '~'state
    '~'action-input
    '~'action-name
    '~'action-params
    '~'activity-options])

(defn new-sci-vars []
  (into {} (for [k (var-syms)]
             [k (sci/new-dynamic-var k nil)])))

(def ^{:doc "Workflow's whole input. Including `code`, `params` for the code`."
       :dynamic true}
  input nil)

(def ^{:doc "input's `:params`, which might be used by `code`."
       :dynamic true}
  params nil)

(def ^{:doc "Should save state on this var."
       :dynamic true}
  state nil)

(def ^{:doc "Workflow action's input"
       :dynamic true}
  action-input nil)

(def ^{:doc "Workflow's messaging action: `:update`, `:signal`, `:query`"
       :dynamic true}
  action-name nil)

(def ^{:doc "Messaging action's params."
       :dynamic true}
  action-params nil)

(def ^{:doc "Default options for building `activity` stub."
       :dynamic true}
  activity-options nil)

(def ^:dynamic sci-vars nil)

(defmacro with-shared-dynamic-vars-bound-to-sci-vars* [var-vals & body]
  `(temporal.sci/with-shared-dynamic-vars-bound-to-sci-vars
     ~(namespace `sci-vars) ~`sci-vars ~(var-syms) ~var-vals ~@body))

(defmacro with-shared-dynamic-vars-copied-out-sci* [& body]
  `(temporal.sci/with-shared-dynamic-vars-copied-out-sci
     ~(namespace `sci-vars) ~`sci-vars ~(var-syms) ~@body))

(defn wrap-fn-with-shared-dynamic-vars-copied-out-sci [f]
  {:pre [sci-vars]}
  (let [sci-vars* sci-vars]
    (fn [& args]
      (binding [sci-vars sci-vars*]
        (with-shared-dynamic-vars-copied-out-sci* (apply f args))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn workflow-info []
  (j/from-java (io.temporal.workflow.Workflow/getInfo)))

(defn sci-wait-condition
  ([duration block-condition]
   (io.temporal.workflow.Workflow/await
    (j/to-java java.time.Duration duration)
    (reify java.util.function.Supplier
      (get [_] (boolean (block-condition))))))
  ([block-condition]
   (io.temporal.workflow.Workflow/await
    (reify java.util.function.Supplier
      (get [_] (boolean (block-condition)))))
   true))

(defn wait-condition
  ([_duration _block-condition] (throw (ex-info "not supported" {})))
  ([_block-condition] (throw (ex-info "not supported" {}))))

(def sci-new-cancellation-scope
  ^:sci/macro
  (fn [_&form _&env & body]
    `(io.temporal.workflow.Workflow/newCancellationScope
      (fn ^:once [] ~@body))))

(defmacro new-cancellation-scope [& body]
  `(let [!cancel# (atom nil)]
     (reify io.temporal.workflow.CancellationScope
       (run [_] ~@body)
       (isDetached [_] false)
       (cancel [_] (reset! !cancel# true))
       (cancel [_ reason#] (reset! !cancel# reason#))
       (getCancellationReason [_]
         (when-let [reason# @!cancel#]
           (when (string? reason#) reason#)))
       (isCancelRequested [_] (boolean @!cancel#))
       (getCancellationRequest [_]))))

(defn sci-async-call-function [f & args]
  (apply (temmporal.workflow.async/to-async-function f) args))

(defn async-call-function [f & args]
  (future (apply f args)))

(defn sci-async-call-procedure [f & args]
  (apply (temmporal.workflow.async/to-async-procedure f) args))

(defn async-call-procedure [f & args]
  (future (apply f args) nil))

(def sci-async
  ^{:sci/macro true}
  (fn [_&form _&env & body]
    `(~`async-call-function
      ^:once (fn [] ~@body))))

(defmacro async [& body]
  `(future ~@body))

(def sci-with-activity-options
  ^:sci/macro
  (fn [_&form _&env options & body]
    `(binding [~`activity-options ~options] ~@body)))

(defmacro with-activity-options [options & body]
  `(binding [~`activity-options ~options] ~@body))

(defn sci-with-sci-activity [submit-f]
  ^:sci/macro
  (fn [_&form _&env & body]
    (let [{:keys [opts activity-options retryable-code code]}
          (let [opts (first body)]
            (if (map? opts)
              {:opts             (dissoc opts :activity-options :retryable)
               :activity-options (:activity-options opts)
               :retryable        (some-> (:retryable opts) pr-str)
               :code             (pr-str `(do ~@(rest body)))}
              {:code (pr-str `(do ~@body))}))

          opts `(cond-> (assoc ~opts :code ~code)
                  ~retryable-code (assoc :retryable ~retryable-code))]
      (if activity-options
        `(~`with-activity-options
          ~activity-options
          (~submit-f "babashka/sci" ~opts))
        `(~submit-f "babashka/sci" ~opts)))))

(defmacro with-sci-activity [params & body]
  `(binding [~'lotuc.sci-rt.temporal.activity/params ~params]
     ~@body))

(defmacro with-sci-activity-async [params & body]
  `(future (binding [~'lotuc.sci-rt.temporal.activity/params ~params]
             ~@body)))

(defn build-activity-stub []
  (when (and (not (:startToCloseTimeout activity-options))
             (not (:scheduleToCloseTimeout activity-options)))
    (throw (ex-info (str "build-activity-stub: BadScheduleActivityAttributes: "
                         "A valid StartToClose or ScheduleToCloseTimeout is not "
                         "set on ScheduleActivityTaskCommand")
                    {:activity-options activity-options
                     :temporal/retryable false})))
  (io.temporal.workflow.Workflow/newUntypedActivityStub
   (j/to-java io.temporal.activity.ActivityOptions activity-options)))

(defn sci-execute-activity [activity-name & args]
  (->> (temporal.csk/transform-named->string args)
       (into-array Object)
       (.execute (build-activity-stub) activity-name Object)
       (temporal.csk/transform-keys->keyword)))

(defn sci-execute-activity-async [activity-name & args]
  (temporal.workflow.promise/promise-then
   (->> (temporal.csk/transform-named->string args)
        (into-array Object)
        (.executeAsync (build-activity-stub) activity-name Object))
   :apply temporal.csk/transform-keys->keyword))

(def ^{:doc "local developing"} !activity-registry (atom {}))

(defn execute-activity [activity-name & args]
  (apply (get @!activity-registry activity-name) args))

(defn execute-activity-async [activity-name & args]
  (future (apply (get @!activity-registry activity-name) args)))

(defn ^{:doc "For exception *explicitly* marked as NOT retryable, rethrow the
  exception with type `DoNotRetryExceptionInfo`."}
  rethrow-inside-retry [e]
  (letfn [(try-rethrow* [e]
            ;; explicitly marked not retryable
            (when-some [b (:temporal/retryable (ex-data e))]
              (when (not b)
                (throw (temporal.ex/->ex-info-do-not-retry e)))))]
    (try-rethrow* e)
    (when (= (:type (ex-data e)) :sci/error)
      (when-some [e (ex-cause e)]
        (try-rethrow* e)))
    (throw e)))

(defn ^{:doc "eliminate the retryable flag"}
  rethrow-outside-retry [t]
  (throw (ex-info (str "retried: " (ex-message t))  (ex-data t) t)))

(defn ^{:doc "`f` do sync run"}
  sci-retry
  ([retry-options f]
   (sci-retry (dissoc retry-options :expiration) (:expiration retry-options) f))
  ([retry-options expiration f]
   (try
     (io.temporal.workflow.Workflow/retry
      (j/to-java io.temporal.common.RetryOptions
                 (update retry-options :doNotRetry (fnil conj [])
                         "lotuc.sci_rt.temporal.DoNotRetryExceptionInfo"))
      (java.util.Optional/ofNullable
       (some->> expiration (j/to-java java.time.Duration)))
      (reify io.temporal.workflow.Functions$Func
        (apply [_]
          (try (f)
               (catch Exception e
                 (rethrow-inside-retry e))))))
     (catch Throwable t
       (rethrow-outside-retry t)))))

(defn ^{:doc "`f` do async run & returns Promise"}
  sci-retry-async
  ([retry-options f]
   (sci-retry-async (dissoc retry-options :expiration) (:expiration retry-options) f))
  ([retry-options expiration f]
   (try (io.temporal.workflow.Async/retry
         (j/to-java io.temporal.common.RetryOptions
                    (update retry-options :doNotRetry (fnil conj [])
                            "lotuc.sci_rt.temporal.DoNotRetryExceptionInfo"))
         (java.util.Optional/ofNullable
          (some->> expiration (j/to-java java.time.Duration)))
         (reify io.temporal.workflow.Functions$Func
           (apply [_]
             (.exceptionally ^io.temporal.workflow.Promise (f)
                             (reify io.temporal.workflow.Functions$Func1
                               (apply [_ e]
                                 (rethrow-inside-retry e)))))))
        (catch Throwable t
          (rethrow-outside-retry t)))))

(defn retry
  ([retry-options f]
   (retry (dissoc retry-options :expiration) (:expiration retry-options) f))
  ([_retry-options _expiration f] (try (f) (catch Throwable e (rethrow-outside-retry e)))))

(defn retry-async
  ([retry-options f]
   (retry-async (dissoc retry-options :expiration) (:expiration retry-options) f))
  ([_retry-options _expiration f] (future (try (f) (catch Throwable e (rethrow-outside-retry e))))))

(def sci-with-retry ^:sci/macro
  (fn [_&form _&env options & body]
    `(let [opts# ~options]
       (~'lotuc.sci-rt.temporal.workflow/retry
        (dissoc opts# :expiration)
        (:expiration opts#)
        ;; do sync run
        ^:once (fn [] ~@body)))))

(def sci-with-retry-async ^:sci/macro
  (fn [_&form _&env options & body]
    `(let [opts# ~options]
       (~'lotuc.sci-rt.temporal.workflow/retry-async
        (dissoc opts# :expiration)
        (:expiration opts#)
        ;; do async run
        ^:once (fn [] (~`async ~@body))))))

(defmacro with-retry [options & body]
  (apply sci-with-retry nil nil options body))

(defmacro with-retry-async [options & body]
  (apply sci-with-retry-async nil nil options body))

(defn promise-completed? [v] (future-done? v))
(defn promise-get-failure [v] (try @v nil (catch Throwable t t)))
(defn promise-get
  ([v] @v)
  ([v duration]
   (let [r (deref v (.toMillis (j/to-java java.time.Duration duration)) ::timeout)]
     (when (= r ::timeout) (throw (java.util.concurrent.TimeoutException. "timeout")))
     r)))

(defn promise-then
  ([v f] (promise-then v :handle f))
  ([v typ f]
   {:pre [#{:handle :apply :compose :handle-exception} typ]}
   (future
     (case typ
       :handle
       (try (f @v nil) (catch Exception e (f nil e)))
       :apply
       (f @v)
       :compose
       @(f @v)
       :handle-exception
       (try @f (catch Exception e (f e)))))))

(defn promise-all-of [promises]
  (when (seq promises)
    (let [res (promise)
          !n (atom (count promises))
          futs
          (doall
           (for [p promises]
             (future (try @p
                          (when (zero? (swap! !n dec))
                            (deliver res nil))
                          (catch Throwable t
                            (deliver res t))))))]
      (try
        (when-some [e @res]
          (throw e))
        (finally (doseq [f futs] (future-cancel f)))))))

(defn promise-any-of [promises]
  (when (seq promises)
    (let [res (promise)
          futs
          (doall
           (for [p promises]
             (future (try (deliver res [@p])
                          (catch Throwable t
                            (deliver res [nil t]))))))]
      (try
        (let [[ok err] @res]
          (if err (throw err) ok))
        (finally (doseq [f futs] (future-cancel f)))))))

(defn sci-fns [sci-vars*]
  (binding [sci-vars sci-vars*]
    (->> {'async-call-function    sci-async-call-function
          'async-call-procedure   sci-async-call-procedure

          'promise-completed?     temporal.workflow.promise/promise-completed?
          'promise-get-failure    temporal.workflow.promise/promise-get-failure
          'promise-get            temporal.workflow.promise/promise-get
          'promise-then           temporal.workflow.promise/promise-then
          'promise-all-of         temporal.workflow.promise/promise-all-of
          'promise-any-of         temporal.workflow.promise/promise-any-of

          'execute-activity       sci-execute-activity
          'execute-activity-async sci-execute-activity-async
          'retry                  sci-retry
          'retry-async            sci-retry-async
          'wait-condition         sci-wait-condition}
         (reduce-kv
          (fn [m k v]
            (assoc m k (wrap-fn-with-shared-dynamic-vars-copied-out-sci v)))
          {}))))

(def sci-macros
  {'new-cancellation-scope  sci-new-cancellation-scope
   'with-activity-options   sci-with-activity-options
   'with-sci-activity       (sci-with-sci-activity `execute-activity)
   'with-sci-activity-async (sci-with-sci-activity `execute-activity-async)
   'with-retry              sci-with-retry
   'with-retry-async        sci-with-retry-async
   'async                   sci-async})
