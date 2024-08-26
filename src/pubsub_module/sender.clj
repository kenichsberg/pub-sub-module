(ns pubsub-module.sender
  #_(:require [clojure.core.async :as a]))


(defprotocol ISender
  (send [this messages])
  )

(defrecord Sender 
  [send-fn]
  ISender
  (send [_ messages]
    (loop [n 0]
      (when (< n 3)
        (when-not (send-fn messages)
          (recur (inc n)))))))
