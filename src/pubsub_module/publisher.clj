(ns pubsub-module.publisher
  (:require [clojure.core.async :as a]))

(defprotocol IPublisher
  (activate [this topic-name ch])
  (publish [this message])
  )

(deftype Publisher 
  [^:volatile-mutable topic-name
   ^:volatile-mutable pub-channel 
   batching-settings]
  IPublisher
  (activate [this topic-name' ch]
    (set! topic-name topic-name')
    (set! pub-channel ch)
    this)

  (publish [_ message]
    (when-not pub-channel (throw (Exception. "Publisher wasn't attached to topic yet.")))
    (a/go 
      ( a/>! pub-channel {:topic-name topic-name
                          :data message}))
    (prn ::pub-dispatched topic-name message)
    nil))

(defn create [batching-settings]
  (->Publisher nil nil batching-settings))
