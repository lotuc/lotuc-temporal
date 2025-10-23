(ns lotuc.sci-rt.temporal.sci
  (:require
   [clojure.java.data :as j]
   [lotuc.sci-rt.temporal.csk :as temporal.csk]
   [lotuc.sci-rt.temporal.ex :as temporal.ex]
   [sci.core :as sci]))

(defn sleep [n]
  (Thread/sleep n))

(defn systemTimeMillis []
  (System/currentTimeMillis))

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

(defn sci-default-namespaces
  ([env] (sci-default-namespaces env nil))
  ([env ^java.util.Random random]
   {:pre [(if (= env :workflow) random true)]}
   {'clojure.java.data
    {'from-java j/from-java
     'from-java-deep j/from-java-deep
     'from-java-shallow j/from-java-shallow}
    'lotuc.sci-rt.temporal.csk
    {'transform-keys temporal.csk/transform-keys
     '->string       temporal.csk/->string
     '->keyword      temporal.csk/->keyword}
    'lotuc.sci-rt.temporal.ex
    {'ex-info-retryable    temporal.ex/ex-info-retryable
     'ex-info-do-not-retry temporal.ex/ex-info-do-not-retry}
    'lotuc.sci-rt.temporal.sci
    (cond-> {'sleep            Thread/sleep
             'systemTimeMillis System/currentTimeMillis}
      (= env :workflow)
      (assoc 'sleep io.temporal.workflow.Workflow/sleep
             'systemTimeMillis io.temporal.workflow.Workflow/currentTimeMillis))
    'clojure.core
    (cond-> {'even? even?
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
             'rand rand}
      (= env :workflow)
      (assoc 'time clojure-core-time
             'deref clojure-core-deref
             'random-uuid io.temporal.workflow.Workflow/randomUUID
             'rand-int #(.nextInt random %)
             'rand (fn
                     ([] (.nextFloat random))
                     ([n] (* n (.nextFloat random))))))}))

(defmacro ^{:doc "Prepare sci bindings for sci evaluation.

  `ns-name` where dynamic variable sits

  `sci-vars` points to a derefable map of symbol name to sci dynamic vars.

  `var-syms` is a compile time vector, which contains *ALL* var names to be
   mapped under `namespace` specified by `sci-vars-sym`

  `var-vals` is a compile time map, which are default values for dynamic var
  bindings."}

  with-shared-dynamic-vars-bound-to-sci-vars
  [ns-name sci-vars var-syms var-vals & sci-eval-code]
  {:pre [(map? var-vals)]}
  (let [ns-name
        (if (string? ns-name) ns-name (name ns-name))]
    (letfn [(->sym [v]
              (if (and (sequential? v) (= (first v) 'quote))
                (second v)
                v))
            (build-redef-bindings [var-sym var-names]
              (mapcat
               (fn [n] [(symbol ns-name (name n))
                        `('~n ~var-sym)])
               var-names))
            (build-sci-bindings [var-vals-sym]
              (mapcat
               (fn [n] [(symbol ns-name (name n))
                        `('~n ~var-vals-sym)])
               (map ->sym (keys var-vals))))]
      (let [vars-sym (gensym "vars_")
            var-vals-sym (gensym "var-vars_")
            bindings (build-redef-bindings vars-sym var-syms)
            sci-bindings (build-sci-bindings var-vals-sym)]
        `(let [~vars-sym ~sci-vars
               ~var-vals-sym ~var-vals]
           (binding [~@bindings]
             (~`sci/binding [~@sci-bindings] ~@sci-eval-code)))))))

(defmacro with-shared-dynamic-vars-copied-out-sci
  [ns-name sci-vars var-syms & native-code]
  (let [ns-name (if (string? ns-name) ns-name (name ns-name))]
    (letfn [(build-bindings [var-sym var-names]
              (mapcat
               (fn [n] [(symbol ns-name (str n))
                        `(deref ('~n ~var-sym))])
               var-names))]
      (let [vars (gensym "vars_")
            bindings (build-bindings vars var-syms)]
        `(let [~vars ~sci-vars]
           (binding [~@bindings]
             ~@native-code))))))
