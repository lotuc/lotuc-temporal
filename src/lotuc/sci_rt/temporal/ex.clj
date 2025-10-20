(ns lotuc.sci-rt.temporal.ex)

(defn ^{:doc "For exception *explicitly* marked as NOT retryable, rethrow the
  exception with type non-retryable `ApplicationFailure`."}
  rethrow-toplevel
  ([throwed]
   (rethrow-toplevel throwed (constantly false)))
  ([throwed retryable?]
   (letfn [(try-rethrow* [t]
             (if (or (:temporal/retryable (ex-data throwed)) (retryable? t))
               (throw t)
               (throw (io.temporal.failure.ApplicationFailure/newNonRetryableFailureWithCause
                       (ex-message t) (.getName (type t)) t (into-array Object [])))))]
     (try-rethrow* throwed)
     (when (= (:type (ex-data throwed)) :sci/error)
       (when-some [e (ex-cause throwed)]
         (try-rethrow* e)))
     (throw throwed))))

(defn ex-info-retryable
  ([msg map]
   (lotuc.sci_rt.temporal.RetryableExceptionInfo. msg map))
  ([msg map cause]
   (lotuc.sci_rt.temporal.RetryableExceptionInfo. msg map cause)))

(defn ex-info-do-not-retry
  ([msg map]
   (lotuc.sci_rt.temporal.DoNotRetryExceptionInfo. msg map))
  ([msg map cause]
   (lotuc.sci_rt.temporal.DoNotRetryExceptionInfo. msg map cause)))

(defn ^{:doc "Build an ex of type `RetryableExceptionInfo`"}
  ->ex-info-retryable [e]
  (if (instance? lotuc.sci_rt.temporal.RetryableExceptionInfo e)
    e
    (throw (ex-info-retryable (ex-message e) (ex-data e) e))))

(defn ^{:doc "Build an ex of type `DoNotRetryExceptionInfo`"}
  ->ex-info-do-not-retry [e]
  (if (instance? lotuc.sci_rt.temporal.DoNotRetryExceptionInfo e)
    e
    (throw (ex-info-do-not-retry (ex-message e) (ex-data e) e))))
