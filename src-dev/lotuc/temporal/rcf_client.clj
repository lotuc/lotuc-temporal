(ns lotuc.temporal.rcf-client
  (:require
   [clojure.java.data :as j]
   [hyperfiddle.rcf :as rcf]
   [lotuc.temporal.client :as c]))

(rcf/tests
 (def r (-> (c/new-workflow-service-stubs)
            (c/new-workflow-client)
            (io.temporal.client.WorkflowClient/.countWorkflows nil)))
 (some? (:count (j/from-java r))) := true

 (def r (-> (c/new-workflow-service-stubs {:target "localhost:7233"})
            (c/new-workflow-client)
            (io.temporal.client.WorkflowClient/.countWorkflows nil)))
 (some? (:count (j/from-java r))) := true

 (def r (-> (c/new-workflow-service-stubs {:target "localhost:7233"})
            (c/new-workflow-client {:namespace "default"})
            (io.temporal.client.WorkflowClient/.countWorkflows nil)))
 (some? (:count (j/from-java r))) := true

 (try
   (-> (c/new-workflow-service-stubs {:target "localhost:7233"})
       (c/new-workflow-client {:namespace "some-random-ns"})
       (io.temporal.client.WorkflowClient/.countWorkflows nil))
   (catch Exception e
     (ex-message e)))
 := "NOT_FOUND: Namespace some-random-ns is not found.")
