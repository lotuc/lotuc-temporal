(ns lotuc.sci-rt.temporal.ex)

(defn actual-cause [e]
  (or (when (= (:type (ex-data e)) :sci/error)
        (ex-cause e))
      e))

(defn ^{:doc "For TemporalException, rethrow as IS. For \"retryable\", rethrow
as IS.

\"retryable\" check:
1. if `:temporal/retryable` mark exists on ex-data, use it
2. else use given `retryable?` function

The check is done on `actual-cause`."}
  rethrow-toplevel
  ([throwed]
   (rethrow-toplevel throwed (constantly false)))
  ([throwed retryable?]
   (letfn [(try-rethrow* [t retryable?]
             (if (instance? io.temporal.failure.TemporalException t)
               (throw t)
               (if-some [b (:temporal/retryable (ex-data t))]
                 (if b
                   (throw t)
                   (throw (io.temporal.failure.ApplicationFailure/newNonRetryableFailureWithCause
                           (str (ex-message t) ": " (ex-data t))
                           (.getName (type t)) throwed (into-array Object []))))
                 (if (retryable? t)
                   (throw t)
                   (throw (io.temporal.failure.ApplicationFailure/newNonRetryableFailureWithCause
                           (str (ex-message t) ": " (ex-data t))
                           (.getName (type t)) throwed (into-array Object [])))))))]
     (try-rethrow* (actual-cause throwed) retryable?))))

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

(defn ^{:doc "continue as new.

  wrap exception with `ex-info-retryable` is an implementation detail. which
  make sure exception pops up to top level without transforming to ApplicationFailure.

  ex-data `:temporal/continue-as-new` being `true` can be considered a reliable
  sign which marks the continue as new exception."}
  ex-continue-as-new
  ([params]
   (ex-info-retryable
    "continue as new"
    {:temporal/continue-as-new true
     :params params
     :state @(resolve 'lotuc.sci-rt.temporal.workflow/state)}))
  ([]
   (ex-info-retryable
    "continue as new"
    {:temporal/continue-as-new true
     :state @(resolve 'lotuc.sci-rt.temporal.workflow/state)})))

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

(defn ex-with-retryable-signature? [ex]
  (contains? (ex-data ex) :temporal/retryable))
