(ns lotuc.sci-rt.temporal.workflow
  (:require
   [clojure.java.data :as j]
   [lotuc.sci-rt.temporal.ex :as temporal.ex]))

(def ^:dynamic params nil)
(def ^:dynamic state nil)

(def ^:dynamic action-name nil)
(def ^:dynamic action-params nil)

(def ^:dynamic activity-options nil)

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

(defn sci-async-run [f]
  (io.temporal.workflow.Async/function
   (reify io.temporal.workflow.Functions$Func
     (apply [_] (f)))))

(defn async-run [f]
  (future (f)))

(def sci-with-activity-options
  ^:sci/macro
  (fn [_&form _&env options & body]
    `(binding [~`activity-options ~options] ~@body)))

(defmacro with-activity-options [options & body]
  `(binding [~`activity-options ~options] ~@body))

(defn sci-with-sci-activity [submit-f]
  ^:sci/macro
  (fn [_&form _&env & body]
    (let [[params retryable-code code]
          (let [opts (first body)]
            (if (map? opts)
              [(:params opts)
               (some-> (:retryable opts) pr-str)
               (pr-str `(do ~@(rest body)))]
              [nil nil (pr-str `(do ~@body))]))]
      `(~submit-f
        "babashka/sci"
        (cond-> {"code" ~code}
          ~params (assoc "params" (~'lotuc.sci-rt.temporal.csk/transform-keys
                                   ~'lotuc.sci-rt.temporal.csk/->string ~params))
          ~retryable-code (assoc "retryable" ~retryable-code))))))

(defmacro with-sci-activity [params & body]
  `(with-bindings [temporal.activity/params ~params]
     ~@body))

(defmacro with-sci-activity-async [params & body]
  `(future (with-bindings [temporal.activity/params ~params]
             ~@body)))

(defn sci-execute-activity [build-activity-stub]
  (fn execute-activity [activity-name & args]
    (.execute (build-activity-stub) activity-name Object (into-array Object args))))

(defn sci-execute-activity-async [build-activity-stub]
  (fn execute-activity-async
    [activity-name & args]
    (let [stub (build-activity-stub)]
      (.executeAsync stub activity-name Object (into-array Object args)))))

(defn execute-activity [_activity-name & _args]
  (throw (ex-info "not supported" {})))

(defn execute-activity-async [_activity-name & _args]
  (throw (ex-info "not supported" {})))

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
  retry
  ([retry-options f]
   (retry (dissoc retry-options :expiration) (:expiration retry-options) f))
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
  retry-async
  ([retry-options f]
   (retry-async (dissoc retry-options :expiration) (:expiration retry-options) f))
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
        ^:once (fn [] (~`async-run ^:once (fn [] ~@body)))))))

(defmacro with-retry [options & body]
  (apply sci-with-retry nil nil options body))

(defmacro with-retry-async [options & body]
  (apply sci-with-retry-async nil nil options body))
