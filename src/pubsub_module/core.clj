(ns pubsub-module.core
  (:require [pubsub-module.topic-manager :as topic-manager]
            [pubsub-module.publisher :as pub]
            [pubsub-module.subscriber :as sub]
            [pubsub-module.persistent-queuing-layer :as queue]
            [pubsub-module.sender :as sender]
            ))

(def create-topic-manager topic-manager/create)

(def create-publisher     pub/create)

(def create-subscriber    sub/create)

(def create-queue-config  queue/create-config)

(def create-sender        sender/->Sender)

