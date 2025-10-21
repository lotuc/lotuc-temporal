(ns lotuc.sci-rt.temporal.env
  (:require
   [sci.core :as sci]))

(def ^{:doc "Will be bound to `:temporal-activity` or `:temporal-workflow` when
started by temporal."
       :dynamic true}
  *env* (sci/new-dynamic-var '*env* :dev))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; activity

(defmacro activity-vars []
  `{:input lotuc.sci-rt.temporal.activity/input
    :params lotuc.sci-rt.temporal.activity/params})

(defmacro with-activity-vars [vars & body]
  `(let [vars# ~vars]
     (binding [lotuc.sci-rt.temporal.activity/input (:input vars#)
               lotuc.sci-rt.temporal.activity/params (:params vars#)]
       ~@body)))

(defmacro with-env-temporal-activity [vars & body]
  `(binding [~`*env* :temporal-activity]
     (~`with-activity-vars ~vars ~@body)))

(defmacro with-env-dev-activity [vars & body]
  `(binding [~`*env* :dev-activity]
     (~`with-activity-vars ~vars ~@body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; workflow

(defmacro workflow-vars []
  `{:input lotuc.sci-rt.temporal.workflow/input
    :params lotuc.sci-rt.temporal.workflow/params
    :state lotuc.sci-rt.temporal.workflow/state

    :action-input lotuc.sci-rt.temporal.workflow/action-input
    :action-name lotuc.sci-rt.temporal.workflow/action-name
    :action-params lotuc.sci-rt.temporal.workflow/action-params

    :activity-options lotuc.sci-rt.temporal.workflow/activity-options})

(defmacro with-workflow-vars [vars & body]
  `(let [vars# ~vars]
     (binding [lotuc.sci-rt.temporal.workflow/input            (:input vars#)
               lotuc.sci-rt.temporal.workflow/params           (:params vars#)
               lotuc.sci-rt.temporal.workflow/state            (:state vars#)

               lotuc.sci-rt.temporal.workflow/action-input     (:action-input vars#)
               lotuc.sci-rt.temporal.workflow/action-name      (:action-name vars#)
               lotuc.sci-rt.temporal.workflow/action-params    (:action-params vars#)

               lotuc.sci-rt.temporal.workflow/activity-options (:activity-options vars#)]
       ~@body)))

(defmacro with-env-temporal-workflow [vars & body]
  `(binding [~`*env* :temporal-workflow]
     (~`with-workflow-vars ~vars ~@body)))

(defmacro with-env-dev-workflow [vars & body]
  `(binding [~`*env* :dev-workflow]
     (~`with-workflow-vars ~vars ~@body)))
