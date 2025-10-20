(ns lotuc.sci-rt.temporal.activity)

(def ^:dynamic params nil)

(defn execution-ctx
  ^io.temporal.activity.ActivityExecutionContext []
  (io.temporal.activity.Activity/getExecutionContext))
