(ns lotuc.sci-rt.temporal.sci)

(defn sleep [n]
  (Thread/sleep n))

(defn systemTimeMillis []
  (System/currentTimeMillis))
