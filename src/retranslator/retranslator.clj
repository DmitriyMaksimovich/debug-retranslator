(ns retranslator.retranslator
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.exchange  :as le]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [clojure.data.json :as json])
  (:use [retranslator.logger :only [write]]))

(def settings (json/read-str (slurp "config/config.json") :key-fn keyword))

(def ^:privat rabbitmq-log-file (:logs-file settings))
(def log (partial write rabbitmq-log-file))

(declare local-rabbitmq publish-localy in? filter-by-hw-type filter-by-message-id filter-by-unit-limit)

(def ^:private msg-filters (:msg-filters settings))

(defn- message-handler [ch {:keys [content-type delivery-tag type] :as meta}
                       ^bytes payload]
  (let [data (json/read-str (String. payload "UTF-8"))
        filtered-data (-> data filter-by-hw-type filter-by-message-id filter-by-unit-limit)]
    (if filtered-data (publish-localy filtered-data))))

(defn- conn-local-rabbitmq [{:keys [host port username password
                                    ex-name ex-autodelete
                                    q-name q-autodelete
                                    message-ttl routing-key]}]
  (let [conn (rmq/connect {:host host :port port :username username :password password})
        ch (lch/open conn)]
    (log [(format "[rabbit-local] Connected. Channel id: %d" (.getChannelNumber ch))])
    (le/declare ch ex-name "topic" {:durable false :auto-delete ex-autodelete})
    (lq/declare ch q-name {:exclusive false :auto-delete q-autodelete :arguments {"x-message-ttl" message-ttl}})
    (lq/bind ch q-name ex-name {:routing-key routing-key})
    (.addShutdownHook (Runtime/getRuntime) (Thread. #(do (rmq/close ch)
                                                         (rmq/close conn)
                                                         (log ["[rabbit-local] Disconnected."]))))
    {:ch ch :conn conn}))

(def ^:private local-rabbitmq (conn-local-rabbitmq (:rabbit-mq-local settings)))

(defn listen-prod-rabbitmq [{:keys [host port username password
                                    ex-name auto-ack
                                    q-name q-autodelete
                                    message-ttl routing-key]}]
  (let [conn (rmq/connect {:host host :port port :username username :password password})
        ch (lch/open conn)]
    (log [(format "[rabbit-prod] Connected. Channel id: %d" (.getChannelNumber ch))])
    (lq/declare ch q-name {:exclusive false :auto-delete q-autodelete :arguments {"x-message-ttl" message-ttl}})
    (lq/bind ch q-name ex-name {:routing-key routing-key})
    (lc/subscribe ch q-name message-handler {:auto-ack auto-ack})
    (.addShutdownHook (Runtime/getRuntime) (Thread. #(do (rmq/close ch)
                                                         (rmq/close conn)
                                                         (log ["[rabbit-prod] Disconnected."]))))))

(defn- publish-localy [message]
  (let [rabbitmq local-rabbitmq]
    (lb/publish (:ch rabbitmq)
                "messages"
                ""
                (json/write-str message)
                {:content-type "text/plain"})))

(defn- in?
  [coll elm]
  (some #(= elm %) coll))

(defn- filter-by-hw-type [data]
  (let [target-hw (:hw-type msg-filters)]
    (if (and data target-hw (= target-hw (-> data :msg :unit_type)))
      data
      nil)))

(def ^:privat unit-ids (atom #{}))

(defn- filter-by-unit-limit [message]
  (if-let [limit (:uniq-unit-limit  msg-filters)]
    (cond
      (in? @unit-ids (:unitId message)) message
      (< (count @unit-ids) limit) (do (swap! unit-ids conj (:unitId message))
                                      message)
      :else nil)
    message))

(defn- filter-by-message-id [message]
  (if-let [id (:unitId message)]
    (if (in? (:units msg-filters) id)
      message)
    nil))
