(ns pubsub-module.topic-manager
  (:require [clojure.core.async :as a]
            [pubsub-module.persistent-queuing-layer :as iqueue]
            [pubsub-module.publisher :as pub]
            [pubsub-module.subscriber :as sub])
  (:import [pubsub_module.persistent_queuing_layer PersistentQueuingLayer QueueConfig]))

(defprotocol ITopicManager
  (add-new-topic [this topic-name topic-type])
  (attach-publisher [this topic-name publisher])
  (attach-subscriber [this topic-name subscriber])
  )

(defrecord TopicManager 
  [^clojure.lang.Atom topic-infos
   ^PersistentQueuingLayer queue
   -pub-channel
   -publication]
  ITopicManager
  (add-new-topic [_ topic-name topic-type]
    (when (get @topic-infos topic-name)
      (throw (Exception. (format "Topic name: '%s' is already used." topic-name))))

    (swap! topic-infos assoc topic-name {:type topic-type
                                         :publishers []
                                         :subscribers []})
    topic-name)

  (attach-publisher [_ topic-name publisher]
    (pub/activate publisher topic-name -pub-channel)
    (swap! topic-infos update-in [topic-name :publishers] conj publisher)
    publisher)

  (attach-subscriber [_ topic-name subscriber]
    (let [sub-channel  (a/chan 100 (.-xform subscriber) (.-ex-handler subscriber)) ]
      (a/sub -publication topic-name sub-channel)
      (sub/activate subscriber sub-channel))
    (swap! topic-infos update-in [topic-name :subscribers] conj subscriber)
    subscriber)
  )

(defn create 
  [^QueueConfig queue-config]
  (let [pub-channel      (a/chan 10000)
        sub-root-channel (a/chan 10000)
        queue            (iqueue/create queue-config pub-channel sub-root-channel)]
    (->TopicManager 
      (atom {})
      queue
      pub-channel
      (a/pub sub-root-channel :topic-name))))
