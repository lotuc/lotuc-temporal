(ns lotuc.sample.temporal.sample-activities)

(definterface ^{io.temporal.activity.ActivityInterface []}
  HelloActivity
  (hello [v]))

(defrecord HelloActivityImpl []
  HelloActivity
  (hello [_ v] (prn :wf-hello-activity v) v))

(def hello-activity HelloActivity)
(def hello-activity-impl HelloActivityImpl)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(definterface ^{io.temporal.activity.ActivityInterface []}
  SimpleMathActivities
  (add [v])
  (delayAdd [v]))

(defrecord SimpleMathActivitiesImpl []
  SimpleMathActivities
  (add [_ {:strs [args]}]
    (apply + args))
  (delayAdd [_ {:strs [opts args]}]
    (Thread/sleep (or (get opts "delayMs") 1000))
    (apply + args)))

(def simple-math-activities SimpleMathActivities)
(def simple-math-activities-impl SimpleMathActivitiesImpl)
