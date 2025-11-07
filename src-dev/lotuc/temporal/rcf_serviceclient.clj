(ns lotuc.temporal.rcf-serviceclient
  (:require
   [clojure.java.data :as j]
   [hyperfiddle.rcf :as rcf]
   [lotuc.temporal.serviceclient :as sc]))

(rcf/tests
 (def r (-> (sc/new-service-stubs)
            (sc/new-workflow-client)
            (io.temporal.client.WorkflowClient/.countWorkflows nil)))
 (some? (:count (j/from-java r))) := true

 (def r (-> (sc/new-service-stubs {:target "localhost:7233"})
            (sc/new-workflow-client)
            (io.temporal.client.WorkflowClient/.countWorkflows nil)))
 (some? (:count (j/from-java r))) := true

 (def r (-> (sc/new-service-stubs {:target "localhost:7233"})
            (sc/new-workflow-client {:namespace "default"})
            (io.temporal.client.WorkflowClient/.countWorkflows nil)))
 (some? (:count (j/from-java r))) := true

 (try
   (-> (sc/new-service-stubs {:target "localhost:7233"})
       (sc/new-workflow-client {:namespace "some-random-ns"})
       (io.temporal.client.WorkflowClient/.countWorkflows nil))
   (catch Exception e
     (ex-message e)))
 := "NOT_FOUND: Namespace some-random-ns is not found.")
