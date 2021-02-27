(ns retranslator.core
  (:gen-class)
  (:use [retranslator.retranslator :only [listen-prod-rabbitmq settings]]))

(defn -main
  []
  (listen-prod-rabbitmq (:rabbit-mq-prod settings)))
