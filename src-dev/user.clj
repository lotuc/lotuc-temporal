(ns user)

(require '[clojure.tools.build.api :as b])

(b/javac
 {:src-dirs ["src"]
  :class-dir "src-classes"
  :basis (b/create-basis {:project "deps.edn"})})

(comment
  (lotuc.sci_rt.temporal.RetryableExceptionInfo. "hello" {})
  (lotuc.sci_rt.temporal.DoNotRetryExceptionInfo. "hello" {})
  #_())
