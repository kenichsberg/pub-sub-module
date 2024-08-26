(ns pubsub-module.subscriber
  (:require [clojure.core.async :as a]
            [pubsub-module.sender :as sender]))

(defprotocol ISubscriber
  (activate [this channel])
  (start [this])
  #_(stop [this])
  )

(deftype Subscriber 
  [^:volatile-mutable sub-channel
   sender
   xform
   ex-handler]
  ISubscriber
  (activate [this channel]
      (set! sub-channel channel)
      (start this)
      this)

  (start [_]
    (when-not sub-channel (throw (Exception. "Subscriber wasn't attached to topic yet.")))
    (a/go-loop []
               (let [messages (a/<! sub-channel)]
                 (sender/send sender messages)
                 (prn ::send-dispatched messages)
                 (recur)))))

(defn create 
  ([sender]
   (create sender [identity] identity))
  ([sender xform-fns]
   (create sender xform-fns identity))
  ([sender xform-fns ex-handler]
   (->Subscriber nil sender (apply comp (reverse xform-fns)) ex-handler)))

