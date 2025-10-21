(ns lotuc.sci-rt.temporal.sci
  (:require
   [sci.core :as sci]))

(defn sleep [n]
  (Thread/sleep n))

(defn systemTimeMillis []
  (System/currentTimeMillis))

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
