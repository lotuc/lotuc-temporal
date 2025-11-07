(ns lotuc.temporal.serviceclient
  (:require
   [clojure.java.data :as j]
   [lotuc.temporal.java-data-builder :as j.builder]))

(set! *warn-on-reflection* true)

(defn new-service-stubs
  (^io.temporal.serviceclient.WorkflowServiceStubs
   []
   (io.temporal.serviceclient.WorkflowServiceStubs/newLocalServiceStubs))
  (^io.temporal.serviceclient.WorkflowServiceStubs
   [options]
   (io.temporal.serviceclient.WorkflowServiceStubs/newServiceStubs
    (if (map? options)
      (j.builder/to-java
       io.temporal.serviceclient.WorkflowServiceStubsOptions
       io.temporal.serviceclient.WorkflowServiceStubsOptions$Builder
       (io.temporal.serviceclient.WorkflowServiceStubsOptions/newBuilder)
       options
       {:valid-setter-return-type? #{io.temporal.serviceclient.ServiceStubsOptions$Builder}})
      (j/to-java io.temporal.serviceclient.WorkflowServiceStubsOptions options)))))

(defn new-workflow-client
  (^io.temporal.client.WorkflowClient
   [^io.temporal.serviceclient.WorkflowServiceStubs service-stubs]
   (io.temporal.client.WorkflowClient/newInstance service-stubs))
  (^io.temporal.client.WorkflowClient
   [^io.temporal.serviceclient.WorkflowServiceStubs service-stubs options]
   (io.temporal.client.WorkflowClient/newInstance
    service-stubs
    (if (map? options)
      (j.builder/to-java
       io.temporal.client.WorkflowClientOptions
       io.temporal.client.WorkflowClientOptions$Builder
       (io.temporal.client.WorkflowClientOptions/newBuilder)
       options
       {:valid-setter-return-type? #{io.temporal.serviceclient.ServiceStubsOptions$Builder}})
      (j/to-java io.temporal.client.WorkflowClientOptions options)))))

(defn new-workflow-stub
  (^io.temporal.client.WorkflowStub
   [^io.temporal.client.WorkflowClient client ^Class clazz workflow-id-or-workflow-options]
   (if (string? workflow-id-or-workflow-options)
     (io.temporal.client.WorkflowClient/.newWorkflowStub
      client clazz ^String workflow-id-or-workflow-options)
     (io.temporal.client.WorkflowClient/.newWorkflowStub
      client clazz
      ^io.temporal.client.WorkflowOptions
      (j.builder/to-java
       io.temporal.client.WorkflowOptions
       io.temporal.client.WorkflowOptions$Builder
       (io.temporal.client.WorkflowOptions/newBuilder)
       workflow-id-or-workflow-options
       {})))))

(defn new-untyped-workflow-stub
  (^io.temporal.client.WorkflowStub
   [^io.temporal.client.WorkflowClient client workflow-id]
   (io.temporal.client.WorkflowClient/.newUntypedWorkflowStub client workflow-id))
  (^io.temporal.client.WorkflowStub
   [^io.temporal.client.WorkflowClient client workflow-id workflow-options]
   (let [^io.temporal.client.WorkflowOptions
         options
         (if (map? workflow-options)
           (j.builder/to-java
            io.temporal.client.WorkflowOptions
            io.temporal.client.WorkflowOptions$Builder
            (io.temporal.client.WorkflowOptions/newBuilder)
            workflow-options
            {})
           (j/to-java io.temporal.client.WorkflowOptions workflow-options))]
     (io.temporal.client.WorkflowClient/.newUntypedWorkflowStub
      client ^String workflow-id options))))
