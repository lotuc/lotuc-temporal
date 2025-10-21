(ns lotuc.sci-rt.temporal.sci
  (:require
   [sci.core :as sci]))

(defn sleep [n]
  (Thread/sleep n))

(defn systemTimeMillis []
  (System/currentTimeMillis))

(defmacro ^{:doc "Prepare sci bindings for sci evaluation.

  `sci-vars-sym` is a namespaced symbol, which points to a derefable map of
  symbol name to sci dynamic vars.

  `var-syms` is a compile time vector, which contains *ALL* var names to be
   mapped under `namespace` specified by `sci-vars-sym`

  `var-vals` is a compile time map, which are default values for dynamic var
  bindings."}

  with-shared-dynamic-vars-bound-to-sci-vars
  [sci-vars-sym var-syms var-vals & sci-eval-code]
  {:pre [(map? var-vals)
         (letfn [(namespaced-sym? [v]
                   (and (symbol? v) (namespace v)))]
           (or (namespaced-sym? sci-vars-sym)
               (and (sequential? sci-vars-sym)
                    (= 'quote (first sci-vars-sym))
                    (namespaced-sym? (second sci-vars-sym)))))]}
  (let [sci-vars-sym
        (if (sequential? sci-vars-sym)
          (second sci-vars-sym)
          sci-vars-sym)
        ns-name
        (name (namespace sci-vars-sym))]
    (letfn [(build-redef-bindings [var-sym var-names]
              (mapcat
               (fn [n] [(symbol ns-name (name n))
                        `('~n ~var-sym)])
               var-names))
            (build-sci-bindings [var-vals-sym default-var-vals-sym]
              (mapcat
               (fn [n] [(symbol ns-name (name n))
                        `(or ('~n ~var-vals-sym)
                             ('~n ~default-var-vals-sym))])
               var-syms))]
      (let [vars-sym (gensym "vars_")
            var-vals-sym (gensym "var-vars_")
            default-var-vals-sym (gensym "default-var-vars_")
            default-var-vals (into {} (for [n var-syms]
                                        `['~n ~(symbol ns-name (name n))]))
            redef-bindings (build-redef-bindings vars-sym var-syms)
            sci-bindings (build-sci-bindings var-vals-sym default-var-vals-sym)]
        `(let [~default-var-vals-sym ~default-var-vals
               ~vars-sym @~sci-vars-sym
               ~var-vals-sym ~var-vals]
           (with-redefs [~@redef-bindings]
             (~`sci/binding [~@sci-bindings] ~@sci-eval-code)))))))

(defmacro with-shared-dynamic-vars-copied-out-sci
  [sci-vars-sym var-syms & native-code]
  {:pre [(letfn [(namespaced-sym? [v]
                   (and (symbol? v) (namespace v)))]
           (or (namespaced-sym? sci-vars-sym)
               (and (sequential? sci-vars-sym)
                    (= 'quote (first sci-vars-sym))
                    (namespaced-sym? (second sci-vars-sym)))))]}
  (let [sci-vars-sym (if (sequential? sci-vars-sym)
                       (second sci-vars-sym)
                       sci-vars-sym)]
    (letfn [(build-bindings [var-sym var-names]
              (mapcat
               (fn [n] [(symbol (name (namespace sci-vars-sym)) (str n))
                        `(deref ('~n ~var-sym))])
               var-names))]
      (let [vars (gensym "vars_")
            bindings (build-bindings vars var-syms)]
        `(let [~vars @~sci-vars-sym]
           (binding [~@bindings]
             ~@native-code))))))

