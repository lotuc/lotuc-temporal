(ns lotuc.sci-rt.temporal.workflow.async
  (:import
   [io.temporal.workflow Async]))

(set! *warn-on-reflection* true)

(defn to-async-function [f]
  (fn
    ([]
     (Async/function
      (reify io.temporal.workflow.Functions$Func
        (apply [_] (f)))))

    ([arg1]
     (Async/function
      (reify io.temporal.workflow.Functions$Func1
        (apply [_ arg1] (f arg1)))
      arg1))

    ([arg1 arg2]
     (Async/function
      (reify io.temporal.workflow.Functions$Func2
        (apply [_ arg1 arg2] (f arg1 arg2)))
      arg1 arg2))

    ([arg1 arg2 arg3]
     (Async/function
      (reify io.temporal.workflow.Functions$Func3
        (apply [_ arg1 arg2 arg3] (f arg1 arg2 arg3)))
      arg1 arg2 arg3))

    ([arg1 arg2 arg3 arg4]
     (Async/function
      (reify io.temporal.workflow.Functions$Func4
        (apply [_ arg1 arg2 arg3 arg4] (f arg1 arg2 arg3 arg4)))
      arg1 arg2 arg3 arg4))

    ([arg1 arg2 arg3 arg4 arg5]
     (Async/function
      (reify io.temporal.workflow.Functions$Func5
        (apply [_ arg1 arg2 arg3 arg4 arg5] (f arg1 arg2 arg3 arg4 arg5)))
      arg1 arg2 arg3 arg4 arg5))

    ([arg1 arg2 arg3 arg4 arg5 arg6]
     (Async/function
      (reify io.temporal.workflow.Functions$Func6
        (apply [_ arg1 arg2 arg3 arg4 arg5 arg6] (f arg1 arg2 arg3 arg4 arg5 arg6)))
      arg1 arg2 arg3 arg4 arg5 arg6))))

(defn to-async-procedure [f]
  (fn
    ([]
     (Async/procedure
      (reify io.temporal.workflow.Functions$Proc
        (apply [_] (f)))))

    ([arg1]
     (Async/procedure
      (reify io.temporal.workflow.Functions$Proc1
        (apply [_ arg1] (f arg1)))
      arg1))

    ([arg1 arg2]
     (Async/procedure
      (reify io.temporal.workflow.Functions$Proc2
        (apply [_ arg1 arg2] (f arg1 arg2)))
      arg1 arg2))

    ([arg1 arg2 arg3]
     (Async/procedure
      (reify io.temporal.workflow.Functions$Proc3
        (apply [_ arg1 arg2 arg3] (f arg1 arg2 arg3)))
      arg1 arg2 arg3))

    ([arg1 arg2 arg3 arg4]
     (Async/procedure
      (reify io.temporal.workflow.Functions$Proc4
        (apply [_ arg1 arg2 arg3 arg4] (f arg1 arg2 arg3 arg4)))
      arg1 arg2 arg3 arg4))

    ([arg1 arg2 arg3 arg4 arg5]
     (Async/procedure
      (reify io.temporal.workflow.Functions$Proc5
        (apply [_ arg1 arg2 arg3 arg4 arg5] (f arg1 arg2 arg3 arg4 arg5)))
      arg1 arg2 arg3 arg4 arg5))

    ([arg1 arg2 arg3 arg4 arg5 arg6]
     (Async/procedure
      (reify io.temporal.workflow.Functions$Proc6
        (apply [_ arg1 arg2 arg3 arg4 arg5 arg6] (f arg1 arg2 arg3 arg4 arg5 arg6)))
      arg1 arg2 arg3 arg4 arg5 arg6))))

(comment
  (do (require '[clojure.string :as string])
      (defn gen-fn [call-name func-name n]
        (letfn [(gen-one [n]
                  (let [args (string/join " " (for [i (range n)]
                                                (str "arg" (inc i))))]

                    (format "
    ([%s]
     (Async/%s
     (reify io.temporal.workflow.Functions$%s%s
        (apply [_ %s] (f %s)))
      %s))

"
                            args call-name
                            func-name (if (zero? n)  "" (str n))
                            args args args)))]
          (print (format "
(defn to-async-%s [f]
  (fn" call-name))
          (doseq [i (range (inc n))]
            (println (gen-one i)))
          (println "
  )
)")))
      (gen-fn "function" "Func" 6)
      (gen-fn "procedure" "Proc" 6)))
