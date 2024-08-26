(ns pubsub-module.persistent-queuing-layer
  (:require [clojure.core.async :as a])
  (:import  [java.util.concurrent  Executors ExecutorService]))

(defrecord QueueConfig 
  [enqueuing-thread-pool-num 
   dequeue-callback-thread-pool-num
   enqueuing-fn
   enqueue-callback-fn
   dequeuing-fn
   batching-settings])

(defn create-config 
  [{:keys [enqueuing-fn             
           dequeuing-fn             
           batching-settings]
    :as config}]
  (assert enqueuing-fn "parameter 'enqueuing-fn' is required")
  (assert dequeuing-fn "parameter 'dequeuing-fn' is required")
  (map->QueueConfig config))


(defprotocol IPersistentQueuingLayer
  (init [this])
  (shutdown-gracefully [this])
  (-flush-queue! [this])
  (-enqueue-async! [this messages])
  (-dequeue-callback [this messages])
  )

(defrecord PersistentQueuingLayer 
  [^ExecutorService enqueuing-thread-pool 
   ^ExecutorService dequeue-callback-thread-pool
   -enqueuing-fn
   -enqueue-callback-fn
   -dequeuing-fn
   -incoming-channel
   -outgoing-channel
   -dequeue-trigger-channel
   -wait-close-channel
   -temp-queue
   -batching-settings]
  IPersistentQueuingLayer
  (init [this]
    ;; enqueuing
    (let [{:keys [msgs-threshlod-num time-threshold-ms]} -batching-settings]
      (a/go-loop [timeout-channel (a/timeout time-threshold-ms)]
                 (let [[message ch]  (a/alts! [-incoming-channel timeout-channel])] 
                   (cond
                     (= ch timeout-channel)
                     (do
                       (-flush-queue! this)
                       (recur (a/timeout time-threshold-ms)))

                     (and message (< msgs-threshlod-num (count @-temp-queue)))
                     (do
                       (-flush-queue! this)
                       (recur (a/timeout time-threshold-ms)))

                     (and message (< (count @-temp-queue) msgs-threshlod-num))
                     (do 
                       (swap! -temp-queue conj message)
                       (recur timeout-channel))

                     (.-closed? -incoming-channel)
                     (do
                       (-enqueuing-fn (seq @-temp-queue))
                       (a/>! -wait-close-channel true))

                     :else
                     (throw (Exception. "Unexpected error"))))))

    ;; dequeuing
    (a/thread
      (when (a/<!! -dequeue-trigger-channel)
        (when-let [dequeued-msgs (-dequeuing-fn)]
          (prn ::dequeued-msgs dequeued-msgs)
          (doseq [[topic-name msgs] dequeued-msgs] 
            (.execute dequeue-callback-thread-pool
                      (fn [] 
                        (-dequeue-callback this 
                                           {:topic-name topic-name
                                            :messages   msgs})))))))
    this)

  (-flush-queue! [this]
    (when-let [messages (seq @-temp-queue)]
      (reset! -temp-queue clojure.lang.PersistentQueue/EMPTY)
      (-enqueue-async! this messages)
      messages))

  (-enqueue-async! [_ messages]
    (.execute enqueuing-thread-pool
              (fn [] 
                (when (-enqueuing-fn messages)
                  (prn ::enqueued messages)
                  (a/>!! -dequeue-trigger-channel true)
                  (when -enqueue-callback-fn
                    (-enqueue-callback-fn))))))

  (-dequeue-callback [_ messages]
    (a/>!! -outgoing-channel messages))

  (shutdown-gracefully [this]
    (println "[Pub/Sub module]: Shutting down gracefully ...")
    (a/close! -dequeue-trigger-channel)
    (a/close! -incoming-channel)

    (a/<!! -wait-close-channel)
    (println "[Pub/Sub module]: Shut down"))
  )

(defn create 
  [^QueueConfig
   {enqueuing-thread-pool-num :enqueuing-thread-pool-num
    dequeue-callback-pool-num :dequeue-callback-pool-num
    enqueuing-fn              :enqueuing-fn
    enqueue-callback-fn       :enqueue-callback-fn
    dequeuing-fn              :dequeuing-fn
    batching-settings         :batching-settings}
   incoming-channel
   outgoing-channel]

  (let [dequeue-trigger-channel (a/chan (a/sliding-buffer 1))
        wait-close-channel      (a/chan (a/sliding-buffer 1))
        temp-queue              (atom clojure.lang.PersistentQueue/EMPTY)]
    (-> (->PersistentQueuingLayer 
          (Executors/newFixedThreadPool (or enqueuing-thread-pool-num 5))
          (Executors/newFixedThreadPool (or dequeue-callback-pool-num 5))
          enqueuing-fn
          enqueue-callback-fn
          dequeuing-fn
          incoming-channel
          outgoing-channel
          dequeue-trigger-channel
          wait-close-channel
          temp-queue
          (or batching-settings
              {:msgs-threshlod-num 1000
               :time-threshold-ms  1000}))
        init)))
