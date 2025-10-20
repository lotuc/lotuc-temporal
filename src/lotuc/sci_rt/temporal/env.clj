(ns lotuc.sci-rt.temporal.env)

(def ^{:doc "Will be bound to `:temporal-activity` or `:temporal-workflow` when
started by temporal."
       :dynamic true}
  *env* :dev)

(defmacro with-env-temporal-activity
  [{:keys [params]} & body]
  `(with-redefs [lotuc.sci-rt.temporal.activity/params ~params]
     (binding [~`*env* :temporal-activity]
       ~@body)))

(defmacro with-env-temporal-workflow [{:keys [params action-name action-params activity-options]} & body]
  `(with-redefs [lotuc.sci-rt.temporal.workflow/params ~params
                 lotuc.sci-rt.temporal.workflow/state nil
                 lotuc.sci-rt.temporal.workflow/activity-options ~activity-options
                 lotuc.sci-rt.temporal.workflow/action-name ~action-name
                 lotuc.sci-rt.temporal.workflow/action-params ~action-params]
     (binding [~`*env* :temporal-workflow]
       ~@body)))

;;

(defmacro with-env-dev-activity [{:keys [params]} & body]
  `(with-redefs [lotuc.sci-rt.temporal.activity/params ~params]
     (binding [~`*env* :dev-activity]
       ~@body)))

(defmacro with-env-dev-workflow [{:keys [params action-name action-params activity-options]} & body]
  `(with-redefs [lotuc.sci-rt.temporal.workflow/params ~params
                 lotuc.sci-rt.temporal.workflow/state nil
                 lotuc.sci-rt.temporal.workflow/activity-options ~activity-options
                 lotuc.sci-rt.temporal.workflow/action-name ~action-name
                 lotuc.sci-rt.temporal.workflow/action-params ~action-params]
     (binding [~`*env* :dev-workflow]
       ~@body)))
