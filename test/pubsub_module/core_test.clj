(ns pubsub-module.core-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as a]
            [pubsub-module.core :as sut]))

(deftest pub-sub-with-http-sender
  (let [mocked-queue-storage  (atom clojure.lang.PersistentQueue/EMPTY)
        mock-enqueue          (fn [msgs]
                                (Thread/sleep 500)
                                (swap! mocked-queue-storage into msgs)
                                :enqueued)
        mock-dequeue          (fn []
                                (Thread/sleep 500)
                                (let [dequeued-msgs (seq @mocked-queue-storage)]
                                  (reset! mocked-queue-storage clojure.lang.PersistentQueue/EMPTY)
                                  (-> (group-by :topic-name dequeued-msgs)
                                      (update-vals #(mapv :data %)))))
        queue-config          (sut/create-queue-config
                                {:enqueuing-fn mock-enqueue
                                 :dequeuing-fn mock-dequeue })
        topic-manager         (sut/create-topic-manager queue-config)

        ;; for topic1
        publisher1            (sut/create-publisher {})
        mocked-endpoint1      (atom clojure.lang.PersistentQueue/EMPTY)
        test-wait-ch1         (a/chan (a/sliding-buffer 1))
        mocked-http-sender1   (sut/create-sender (fn [msg] 
                                                   (Thread/sleep 500)
                                                   (swap! mocked-endpoint1 conj msg)
                                                   (a/>!! test-wait-ch1 true)
                                                   :message-sent))
        subscriber1           (sut/create-subscriber mocked-http-sender1)

        ;; for topic2
        publisher2            (sut/create-publisher {})
        mocked-endpoint2      (atom clojure.lang.PersistentQueue/EMPTY)
        test-wait-ch2         (a/chan (a/sliding-buffer 1))
        mocked-http-sender2   (sut/create-sender (fn [msg] 
                                                   (Thread/sleep 500)
                                                   (swap! mocked-endpoint2 conj msg)
                                                   (a/>!! test-wait-ch2 true)
                                                   :message-sent))
        subscriber2           (sut/create-subscriber mocked-http-sender2)]

    #_(do 
      (def _q mocked-queue-storage)
      (def _e1 mocked-endpoint1)
      (def _e2 mocked-endpoint2))

    ;; topic1
    (.add-new-topic topic-manager :topic-1 :with-queue-storage)
    (.attach-publisher topic-manager :topic-1 publisher1)
    (.attach-subscriber topic-manager :topic-1 subscriber1)

    ;; topic2
    (.add-new-topic topic-manager :topic-2 :with-queue-storage)
    (.attach-publisher topic-manager :topic-2 publisher2)
    (.attach-subscriber topic-manager :topic-2 subscriber2)


    (testing "smoke"
      (.publish publisher1 "foo")
      (.publish publisher2 "bar")

      (a/<!! test-wait-ch1)
      (is (= '({:topic-name :topic-1
                :messages ["foo"]})
             (seq @mocked-endpoint1)))

      (a/<!! test-wait-ch2)
      (is (= '({:topic-name :topic-2
                :messages ["bar"]})
             (seq @mocked-endpoint2)))

      (is (= nil (seq @mocked-queue-storage)))
      )))

(comment
  (deref _q)
  (reset! _q (atom clojure.lang.PersistentQueue/EMPTY))
  )
