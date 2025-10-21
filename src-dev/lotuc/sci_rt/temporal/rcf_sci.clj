(ns lotuc.sci-rt.temporal.rcf-sci
  (:require
   [hyperfiddle.rcf :as rcf]
   [lotuc.sci-rt.temporal.sci :as temporal.sci]
   [sci.core :as sci]))

(rcf/enable!)

(defmacro ^{:doc "A compile time list of sci shared dynamic vars."}
  sci-var-names []
  `['~'v '~'v1])

(def sci-vars (delay (into {} (for [k (sci-var-names)]
                                [k (sci/new-dynamic-var k nil)]))))

(def ^:dynamic v  :v-init)

(def ^:dynamic v1 :v1-init)

(defmacro with-shared-dynamic-vars-bound-to-sci-vars* [var-vals & body]
  `(temporal.sci/with-shared-dynamic-vars-bound-to-sci-vars
     `sci-vars ~(sci-var-names) ~var-vals ~@body))

(defmacro with-shared-dynamic-vars-copied-out-sci* [& body]
  `(temporal.sci/with-shared-dynamic-vars-copied-out-sci
     `sci-vars ~(sci-var-names) ~@body))

(defn wrap-fn-with-shared-dynamic-vars-copied-out-sci [f]
  (fn [& args]
    (with-shared-dynamic-vars-copied-out-sci* (apply f args))))

(defn get-v [] v)

(def !store (atom []))

(defn store-clear! [] (reset! !store []))

(defn store-append! [v] (swap! !store conj v))

;; setup shared dynamic vars for sci evaluation
;; - replace dynamic bindings with
(temporal.sci/with-shared-dynamic-vars-bound-to-sci-vars
  `sci-vars [v v1] {'v 42}
  (-> (sci/init
       {:namespaces
        {'user {'store-clear! store-clear!
                'store-append! store-append!
                'v v
                'v1 v1
                ;; for native functions which use shared dynamic bindings.
                'get-v
                (fn []
                  (temporal.sci/with-shared-dynamic-vars-copied-out-sci `sci-vars [v v1] (get-v)))}
         'clojure.core {'prn prn}}})
      (sci/eval-form
       '(do
          (store-clear!)
          (store-append! v1)              ; v1 initialized by its default value
          (store-append! v)               ; v initialized to 42
          (store-append! (get-v))         ; go back to native & retrieve v
          (binding [user/v :bound-in-sci] ; testing binding from sci
            (store-append! v)             ; bound value in sci
            (store-append! (get-v))       ; bound value in native code
            #_())))))

(rcf/tests
 @!store := [:v1-init 42 42 :bound-in-sci :bound-in-sci])

;;; testing the helper wrapper for specified namespace

(with-shared-dynamic-vars-bound-to-sci-vars*
  {'v 42}
  (-> (sci/init
       {:namespaces
        {'user {'store-clear! store-clear!
                'store-append! store-append!
                'v v
                'v1 v1
                'get-v (wrap-fn-with-shared-dynamic-vars-copied-out-sci get-v)}
         'clojure.core {'prn prn}}})
      (sci/eval-form
       '(do
          (store-clear!)
          (store-append! v1)           ; v1 initialized by its default value
          (store-append! v)            ; v initialized to 42
          (store-append! (get-v))      ; go back to native & retrieve v
          (binding [user/v :bound-in-sci] ; testing binding from sci
            (store-append! v)             ; bound value in sci
            (store-append! (get-v))       ; bound value in native code
            #_())))))

(rcf/tests
 @!store := [:v1-init 42 42 :bound-in-sci :bound-in-sci])
