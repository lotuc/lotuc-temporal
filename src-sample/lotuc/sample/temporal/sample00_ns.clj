(ns lotuc.sample.temporal.sample00-ns)

;;; 1. https://docs.temporal.io/develop/java/namespaces

(def service-stubs
  (io.temporal.serviceclient.WorkflowServiceStubs/newLocalServiceStubs))
(def service-blocking-stub
  (.blockingStub service-stubs))

;;; ** Register a Namespace

;;; https://docs.temporal.io/develop/java/namespaces#register-namespace

(def create-req
  (-> (io.temporal.api.workflowservice.v1.RegisterNamespaceRequest/newBuilder)
      (.setNamespace "ns42")
      (.setWorkflowExecutionRetentionPeriod (com.google.protobuf.util.Durations/fromDays 1))
      (.build)))

(comment
  (def create-resp
    (.registerNamespace service-blocking-stub create-req))
  (bean create-resp)
  #_())

;;; ** Manage Namespaces

;;; https://docs.temporal.io/develop/java/namespaces#manage-namespaces

;;; update
(def update-req
  (-> (io.temporal.api.workflowservice.v1.UpdateNamespaceRequest/newBuilder)
      (.setNamespace "ns42")
      (.setUpdateInfo
       (-> (io.temporal.api.namespace.v1.UpdateNamespaceInfo/newBuilder)
           (.setDescription "Namespace ns42")
           (.build)))
      (.setConfig
       (-> (io.temporal.api.namespace.v1.NamespaceConfig/newBuilder)
           (.setWorkflowExecutionRetentionTtl (com.google.protobuf.util.Durations/fromHours 30))
           (.build)))
      (.build)))

(comment
  (def update-resp
    (.updateNamespace service-blocking-stub update-req))
  (bean update-resp)
  #_())

;;; describe
(def describe-req
  (-> (io.temporal.api.workflowservice.v1.DescribeNamespaceRequest/newBuilder)
      (.setNamespace "ns42")
      (.build)))

(comment
  (->> (.describeNamespace service-blocking-stub describe-req)
       (bean)
       (:namespaceInfo) (bean))
  #_())

;;; listing
(def list-req
  (-> (io.temporal.api.workflowservice.v1.ListNamespacesRequest/newBuilder)
      (.build)))

(comment
  (-> (.listNamespaces service-blocking-stub list-req)
      (bean)
      (select-keys [:namespacesList :namespacesCount])
      (update :namespacesList #(map (comp bean :namespaceInfo bean) %))
      #_())
  #_())

;;; deprecate
(def deprecate-req
  (-> (io.temporal.api.workflowservice.v1.DeprecateNamespaceRequest/newBuilder)
      (.setNamespace "ns42")
      (.build)))

(comment
  (def deprecate-resp (.deprecateNamespace service-blocking-stub deprecate-req))
  (bean deprecate-resp)

  (->> (.describeNamespace service-blocking-stub describe-req)
       (bean)
       (:namespaceInfo) (bean) :state (.name)) ;=> "NAMESPACE_STATE_DEPRECATED"
  #_())

;;; delete
(def delete-req
  (-> (io.temporal.api.operatorservice.v1.DeleteNamespaceRequest/newBuilder)
      (.setNamespace "ns42")
      (.build)))

(comment
  (def delete-resp
    (-> (io.temporal.serviceclient.OperatorServiceStubs/newServiceStubs
         (-> (io.temporal.serviceclient.OperatorServiceStubsOptions/newBuilder)
             (.setChannel (.getRawChannel service-stubs))
             (.validateAndBuildWithDefaults)))
        (.blockingStub)
        (.deleteNamespace delete-req)))
  (bean delete-resp)
  #_())
