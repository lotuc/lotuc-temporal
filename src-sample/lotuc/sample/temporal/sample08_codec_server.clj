(ns lotuc.sample.temporal.sample08-codec-server
  (:require
   [clojure.java.io :as io]
   [integrant.core :as ig]
   [integrant.repl :as ig.repl]
   [lotuc.sample.temporal.sample-converter :as sample-converter]
   [org.httpkit.client :as hk-client]
   [org.httpkit.server :as hk-server]))

;;; 1. https://docs.temporal.io/codec-server
;;; 2. https://github.com/temporalio/sdk-java/blob/master/temporal-remote-data-encoder/README.md

(def json-format (com.google.protobuf.util.JsonFormat/parser))
(def json-printer (com.google.protobuf.util.JsonFormat/printer))

(defn json->payloads ^io.temporal.api.common.v1.Payloads [json]
  (let [incoming-payload (io.temporal.api.common.v1.Payloads/newBuilder)]
    (with-open [r (if (instance? String json)
                    (io/reader (java.io.StringReader. json))
                    (io/reader json))]
      (.merge json-format r incoming-payload))
    (.build incoming-payload)))

(defn payloads->json [^io.temporal.api.common.v1.Payloads v]
  (with-out-str (.appendTo json-printer v *out*)))

(def nippy-codec (sample-converter/map->NippyCodec {}))

(def codec
  (io.temporal.payload.codec.ChainCodec.
   [nippy-codec]))

(defn codec-convert
  ^io.temporal.api.common.v1.Payloads
  [^io.temporal.api.common.v1.Payloads payloads encode?]
  (-> (io.temporal.api.common.v1.Payloads/newBuilder)
      (.addAllPayloads
       (let [v (.getPayloadsList payloads)]
         (if encode?
           (.encode codec v)
           (.decode codec v))))
      (.build)))

(defn app [req]
  (case (:request-method req)
    :post
    {:status  200
     :headers {"Content-Type" "application/json"
               "Access-Control-Allow-Origin" "*"}
     :body    (let [encode? (case (:uri req)
                              "/encode" true
                              "/decode" false)]
                (-> (json->payloads (:body req))
                    (codec-convert encode?)
                    (payloads->json)))}
    :options
    {:status  200
     :headers {"Access-Control-Allow-Origin" "*"
               "Access-Control-Allow-Methods" "*"
               "Access-Control-Allow-Headers" "*"}
     :body    ""}))

(defmethod ig/init-key ::server [_ opts]
  (hk-server/run-server #'app (update opts :port #(or % 8080))))

(defmethod ig/halt-key! ::server [_ server] (server))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def port 8181)

;; Configure Codec Server to be `http://localhost:8081`
;;  - the glass button on top right of temporal UI.

(defn endpoint [encode?]
  (format "http://localhost:%s/%s" port (if encode? "encode" "decode")))

(ig.repl/set-prep!
 #(ig/expand
   {::server {:port port}}))

(def nippy-converter (sample-converter/map->NippyConverter {}))

(defn converter-convert
  [v encode?]
  (if encode?
    (.get (.toData nippy-converter v))
    (cond
      (instance? io.temporal.api.common.v1.Payload v)
      (.fromData nippy-converter v Object nil)
      (instance? io.temporal.api.common.v1.Payloads v)
      (mapv #(.fromData nippy-converter % Object nil)
            (.getPayloadsList v))
      :else (throw (ex-info "unsupported type" {:v v})))))

(comment
  (def test-payloads
    (-> (io.temporal.api.common.v1.Payloads/newBuilder)
        (.addAllPayloads
         [(converter-convert {:hello "world"} true)])
        (.build)))

  (-> (payloads->json (codec-convert test-payloads true))
      (json->payloads)
      (codec-convert false)
      (converter-convert false))

  (-> @(hk-client/post
        (endpoint false)
        {:body (-> (codec-convert test-payloads true)
                   (payloads->json))})
      (:body)
      (json->payloads)
      (converter-convert false))

  (-> @(hk-client/post
        (endpoint true)
        {:body (payloads->json test-payloads)})
      (:body)
      (json->payloads)
      (codec-convert false)
      (converter-convert false))

  #_())
