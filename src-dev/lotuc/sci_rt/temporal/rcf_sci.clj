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
     ~(namespace `sci-vars) @~`sci-vars ~(sci-var-names) ~var-vals ~@body))

(defmacro with-shared-dynamic-vars-copied-out-sci* [& body]
  `(temporal.sci/with-shared-dynamic-vars-copied-out-sci
     ~(namespace `sci-vars) @~`sci-vars ~(sci-var-names) ~@body))

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
  lotuc.sci-rt.temporal.rcf-sci @sci-vars [v v1] {'v 42}
  (-> (sci/init {:namespaces
                 {'user {'store-clear! store-clear!
                         'store-append! store-append!

                           ;; v & v1 is being local bound by `with-shared-dynamic-vars-bound-to-sci-vars`
                         'v v
                         'v1 v1

                           ;; for native functions which use shared dynamic bindings.
                         'get-v
                         (fn []
                           (temporal.sci/with-shared-dynamic-vars-copied-out-sci
                             lotuc.sci-rt.temporal.rcf-sci
                             @sci-vars [v v1] (get-v)))}
                  'clojure.core {'prn prn}}})
      (sci/eval-form
       '(do
          (store-clear!)
          (store-append! v1)   ; value *WONT* be initialize with the native
                                        ; binding it will be initialize with `sci-var`,
                                        ; which might across multiple sci evaluation.
          (store-append! v)           ; v initialized to 42
          (store-append! (get-v))     ; go back to native & retrieve v
          (binding [user/v :bound-in-sci] ; testing binding from sci
            (store-append! v)             ; bound value in sci
            (store-append! (get-v))       ; bound value in native code
            #_())))))

(rcf/tests
 @!store := [nil 42 42 :bound-in-sci :bound-in-sci])

;;; testing the helper wrapper for specified namespace

(store-clear!)

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
          (store-append! v1)
          (alter-var-root #'v1 (constantly :updated-v1))
          (store-append! v1)
          (store-append! v)            ; v initialized to 42
          (store-append! (get-v))      ; go back to native & retrieve v
          (binding [user/v :bound-in-sci] ; testing binding from sci
            (store-append! v)             ; bound value in sci
            (store-append! (get-v))       ; bound value in native code
            #_())))))

(rcf/tests
 @!store := [nil :updated-v1 42 42 :bound-in-sci :bound-in-sci])

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
          (store-append! v1)            ; value kept across multiple run
          ))))

(rcf/tests
 @!store := [:updated-v1])
