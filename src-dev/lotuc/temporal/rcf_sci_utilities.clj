(ns lotuc.temporal.rcf-sci-utilities
  (:require
   [hyperfiddle.rcf :as rcf]
   [lotuc.sci-rt.temporal.workflow :as temporal.workflow]
   [lotuc.temporal.sci]
   [sci.core :as sci]))

(rcf/enable!)

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
