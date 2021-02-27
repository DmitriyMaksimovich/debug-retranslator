(ns retranslator.logger
  (:require [clojure.java.io :as io])
  (:import java.util.Date
           java.util.TimeZone
           java.text.SimpleDateFormat))

(defn- create-dt-formatter-with-time-zone [time-format time-zone-code]
  (let [*date-format* (SimpleDateFormat. time-format)
        time-zone (TimeZone/getTimeZone time-zone-code)]
    (.setTimeZone *date-format* time-zone)
    *date-format*))

(defn date-formatter
  ([] (create-dt-formatter-with-time-zone "MM.dd HH:mm:ss" "UTC"))
  ([time-zone] (create-dt-formatter-with-time-zone "MM.dd HH:mm:ss" time-zone))
  ([time-zone time-format] (create-dt-formatter-with-time-zone time-format time-format)))

(def ^:private default-formatter (date-formatter "UTC" "MM/dd HH:mm:ss:SSSSS"))

(defn- date-to-string
  ([^Date date] (let [formatter (date-formatter)] (.format formatter date)))
  ([^Date date ^SimpleDateFormat formatter] (.format formatter date)))

(defn write [file data & {:keys [formatter]
                          :or {formatter default-formatter}}]
  (with-open [^java.io.Writer writer (io/writer file :append true)]
    (let [formatter formatter
          date-now (date-to-string (Date.) formatter)]
      (doall (map #(.write writer (str date-now " " % "\n")) data)))))

;; ---------- 2.0 ------------

;; (defn logger [writer] #(.write writer %))
;; (def log->console (logger *out*))
;; (log->console "data")

;; (defn file-logger [file]
;;   #(with-open [writer (io/writer file :append true)]
;;      ((logger writer) %)))
;; (defn log->file [file] (file-logger file))
;; (def my-serv-log "logs/text.log")
;; (def service-logger (log->file my-serv-log))

;; ;; ------ tests -------

;; (def log-file "logs/text.log")
;; (def my-formatter (date-formatter "UTC" "MM/dd HH:mm:ss:SSSSS"))

;; (write log-file ["test" "test1" "test4"] :formatter my-formatter)

;; (service-logger (str (date-to-string (Date.) default-formatter) " " "data3" "\n"))
