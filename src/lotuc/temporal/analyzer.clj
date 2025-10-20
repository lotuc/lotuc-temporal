(ns lotuc.temporal.analyzer
  (:require
   [clojure.pprint]
   [clojure.tools.analyzer.jvm :as analyzer.jvm]
   [clojure.tools.analyzer.passes.jvm.emit-form :as jvm.emit-form]))

(defn ^{:doc "https://stackoverflow.com/questions/48533064/how-do-you-find-all-the-free-variables-in-a-clojure-expression"}
  find-free-variables [form]
  (let [free-variables (atom #{})]
    (letfn [(save-and-replace-with-nil [_ s _]
              (swap! free-variables conj s)
              ;; replacing unresolved symbol with `nil`
              ;; in order to keep AST valid
              {:op :const
               :env {}
               :type :nil
               :literal? true
               :val nil
               :form nil
               :top-level true
               :o-tag nil
               :tag nil})]
      (analyzer.jvm/analyze
       form
       (analyzer.jvm/empty-env)
       {:passes-opts
        (assoc analyzer.jvm/default-passes-opts
               :validate/unresolvable-symbol-handler
               save-and-replace-with-nil)})
      @free-variables)))

(comment
  (let [a 42]
    (-> (analyzer.jvm/analyze
         '(+ a 42)
         (analyzer.jvm/empty-env)
         {:passes-opts
          (-> analyzer.jvm/default-passes-opts
              (assoc
               :validate/unresolvable-symbol-handler
               (fn [_ s _]
                 {:op :const
                  :env {}
                  :type :symbol
                  :literal? true
                  :val s
                  :form 'fdjkafdj
                  :top-level true
                  :o-tag nil
                  :tag nil})))})
        jvm.emit-form/emit-form)))
