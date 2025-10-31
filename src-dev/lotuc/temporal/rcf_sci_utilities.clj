(ns lotuc.temporal.rcf-sci-utilities
  (:require
   [hyperfiddle.rcf :as rcf]
   [lotuc.sci-rt.temporal.ex :as temporal.ex]
   [lotuc.sci-rt.temporal.workflow :as temporal.workflow]
   [lotuc.temporal.sci :refer [with-sci-code]]
   [sci.core :as sci]))

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

(comment
  (lotuc.temporal.sci/alter-preset-namespaces! :workflow (constantly {}))
  (lotuc.temporal.sci/alter-preset-namespaces! :activity (constantly {})))

(with-redefs [lotuc.temporal.sci/!sci-workflow-preset-namespaces (atom {})
              lotuc.temporal.sci/!sci-activity-preset-namespaces (atom {})]
  (rcf/tests
   "register preset namespaces"

   (try (-> (lotuc.temporal.sci/load-preset-namespaces
             :workflow {} {'clojure.core ['abs]}))
        (catch Throwable t (:temporal/retryable (ex-data t))))
   := false

   (try (-> (lotuc.temporal.sci/load-preset-namespaces
             :activity {} {'clojure.core ['abs]}))
        (catch Throwable t (:temporal/retryable (ex-data t))))
   := false

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; register to workflow runtime ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

   (lotuc.temporal.sci/alter-preset-namespaces!
    :workflow assoc 'clojure.core {'abs abs})

    ;; var-sym -> array of syms
   (-> (lotuc.temporal.sci/load-preset-namespaces :workflow {} {'clojure.core ['abs]})
       (get-in ['clojure.core 'abs]) (some?))
   := true

;; var-sym -> sym : load single var
   (-> (lotuc.temporal.sci/load-preset-namespaces :workflow {} {'clojure.core 'abs})
       (get-in ['clojure.core 'abs]) (some?))
   := true

   (->> (for [f [keyword symbol str]
              p [:key :val :both]
              :let [f* (fn [p* v] (if (or (= p :both) (= p p*)) (f v) v))]]
          (lotuc.temporal.sci/load-preset-namespaces
           :workflow {} {(f* :key "clojure.core") (f* :val "all")}))
        (mapv #(-> (get-in % ['clojure.core 'abs]) (some?)))
        (every? identity))
   := true

   (try (-> (lotuc.temporal.sci/load-preset-namespaces :activity {} {'clojure.core ['abs]}))
        (catch Throwable t (:temporal/retryable (ex-data t))))
   := false

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; register to activity runtime ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

   (lotuc.temporal.sci/alter-preset-namespaces! :activity assoc 'clojure.core {'abs abs})

   (-> (lotuc.temporal.sci/load-preset-namespaces :workflow {} {'clojure.core ['abs]})
       (get-in ['clojure.core 'abs]) (some?))
   := true

   (-> (lotuc.temporal.sci/load-preset-namespaces :activity {} {'clojure.core ['abs]})
       (get-in ['clojure.core 'abs]) (some?))
   := true

   #_()))

(with-redefs [lotuc.temporal.sci/!sci-workflow-preset-namespaces (atom {})
              lotuc.temporal.sci/!sci-activity-preset-namespaces (atom {})]

  (rcf/tests
   "native function gets the shared var"

   (defn get-workflow-params []
     temporal.workflow/params)

   (lotuc.temporal.sci/alter-preset-namespaces!
    :workflow assoc 'user {'get-workflow-params get-workflow-params})

   (sci/eval-form
    (sci/init
     {:namespaces
      (lotuc.temporal.sci/build-sci-workflow-ns
       (temporal.workflow/new-sci-vars)
       {'user 'all}
       (java.util.Random.))})
    '(binding [lotuc.sci-rt.temporal.workflow/params 42]
       [lotuc.sci-rt.temporal.workflow/params
        (get-workflow-params)]))
   := [42 42]))
