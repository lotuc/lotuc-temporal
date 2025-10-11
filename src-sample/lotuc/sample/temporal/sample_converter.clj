(ns lotuc.sample.temporal.sample-converter
  (:require
   [taoensso.encore :as encore]
   [taoensso.nippy :as nippy]))

(def encoding-type-nippy
  (com.google.protobuf.ByteString/copyFrom "nippy" "UTF-8"))

(def encoding-type-meta-key
  io.temporal.common.converter.EncodingKeys/METADATA_ENCODING_KEY)

(defrecord NippyCodec [freeze-opts thaw-opts default-opts]
  io.temporal.payload.codec.PayloadCodec
  (encode [_ payloads]
    (let [opts (encore/nested-merge default-opts freeze-opts)]
      (try
        (map (fn [payload]
               (let [data (-> (.toByteArray payload)
                              (nippy/freeze opts)
                              (com.google.protobuf.ByteString/copyFrom))]
                 (-> (io.temporal.api.common.v1.Payload/newBuilder)
                     (.putMetadata encoding-type-meta-key encoding-type-nippy)
                     (.setData data)
                     (.build))))
             payloads)
        (catch Throwable t
          (throw (io.temporal.common.converter.DataConverterException. t))))))
  (decode [_ payloads]
    (let [opts (encore/nested-merge default-opts thaw-opts)]
      (try
        (map (fn [payload]
               (if (= encoding-type-nippy
                      (.getMetadataOrDefault payload encoding-type-meta-key nil))
                 (-> (.. payload (getData) (toByteArray))
                     (nippy/thaw opts)
                     (io.temporal.api.common.v1.Payload/parseFrom))
                 payload))
             payloads)
        (catch Throwable t
          (throw (io.temporal.payload.codec.PayloadCodecException. t)))))))

(defrecord NippyConverter [freeze-opts thaw-opts default-opts]
  io.temporal.common.converter.PayloadConverter
  (getEncodingType [_] "nippy")
  (toData [this v]
    (java.util.Optional/of
     (let [opts (encore/nested-merge default-opts freeze-opts)
           data (nippy/freeze v opts)]
       (-> (io.temporal.api.common.v1.Payload/newBuilder)
           (.putMetadata encoding-type-meta-key
                         (com.google.protobuf.ByteString/copyFrom
                          (.getEncodingType this) "UTF-8"))
           (.setData (com.google.protobuf.ByteString/copyFrom data))
           (.build)))))
  (fromData [_ payload _value-class _value-type]
    (let [opts (encore/nested-merge default-opts thaw-opts)]
      (nippy/thaw (.. payload (getData) (toByteArray)) opts))))
