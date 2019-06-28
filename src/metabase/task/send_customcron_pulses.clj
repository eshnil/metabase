(ns metabase.task.send-customcron-pulses
  "Tasks related to running `CustomCron Pulses`."
  (:require [clj-time
             [core :as time]
             [predicates :as timepr]]
            [clojure.tools.logging :as log]
            [clojurewerkz.quartzite
             [jobs :as jobs]
             [triggers :as triggers]]
            [clojurewerkz.quartzite.schedule.cron :as cron]
            [metabase
             [pulse :as p]
             [task :as task]]
            [metabase.models
             [pulse :as pulse]
             [pulse-channel :as pulse-channel]
             [setting :as setting]
             [task-history :as task-history]]
            [metabase.util.i18n :refer [trs]]
            [environ.core :as environ]
            [schema.core :as s]))

;;; ------------------------------------------------- PULSE SENDING --------------------------------------------------

(defn- log-pulse-exception [pulse-id exception]
  (log/error exception (trs "Error sending Pulse {0}" pulse-id)))

(def customcron-frequency-minutes
  "At what frequency should Metabase wake up to send all the customcron jobs.
  Default is once per hour. Use a value like 0,15,30,45 for once every 15minutes."
  (or (:mb-customcron-freq-mins environ/env) "0"))

(s/defn ^:private send-customcron-pulses!
  "Send any `Pulses` which are scheduled to run in the current day/hour."
  ([now]
   (send-customcron-pulses! now log-pulse-exception))

  ([now on-error]
   (log/info (trs "Sending customcron pulses..."))
   (let [pulse-id->channels (group-by :pulse_id (pulse-channel/retrieve-customcron-channels now))]
     (doseq [[pulse-id channels] pulse-id->channels]
       (try
         (task-history/with-task-history {:task (format "send-customcron-pulse %s" pulse-id)}
           (log/debug (trs "Starting Pulse Execution: {0}" pulse-id))
           (when-let [pulse (pulse/retrieve-notification pulse-id :archived false)]
             (p/send-pulse! pulse :channel-ids (map :id channels)))
           (log/debug (trs "Finished Pulse Execution: {0}" pulse-id)))
         (catch Throwable e
           (on-error pulse-id e)))))))


;;; ------------------------------------------------------ Task ------------------------------------------------------

;; triggers the sending of all pulses which are scheduled to run in the current hour
(jobs/defjob SendCustomcronPulses [_]
  (try
    (task-history/with-task-history {:task "send-customcron-pulses"}
      ;; determine what time it is right now (hour-of-day & day-of-week) in reporting timezone
      (let [reporting-timezone (setting/get :report-timezone)
            now                (if (empty? reporting-timezone)
                                 (time/now)
                                 (time/to-time-zone (time/now) (time/time-zone-for-id reporting-timezone)))]
        (send-customcron-pulses! now)))
    (catch Throwable e
      (log/error e (trs "SendCustomcronPulses task failed")))))

(def ^:private send-customcron-pulses-job-key     "metabase.task.send-customcron-pulses.job")
(def ^:private send-customcron-pulses-trigger-key "metabase.task.send-customcron-pulses.trigger")

(defmethod task/init! ::SendCustomcronPulses [_]
  (let [job     (jobs/build
                 (jobs/of-type SendCustomcronPulses)
                 (jobs/with-identity (jobs/key send-customcron-pulses-job-key)))
        trigger (triggers/build
                 (triggers/with-identity (triggers/key send-customcron-pulses-trigger-key))
                 (triggers/start-now)
                 (triggers/with-schedule
                   (cron/schedule
                    ;; run at the top of every hour
                    (cron/cron-schedule (str "0 " customcron-frequency-minutes " * * * ?"))
                    ;; If send-pulses! misfires, don't try to re-send all the misfired Pulses. Retry only the most
                    ;; recent misfire, discarding all others. This should hopefully cover cases where a misfire
                    ;; happens while the system is still running; if the system goes down for an extended period of
                    ;; time we don't want to re-send tons of (possibly duplicate) Pulses.
                    ;;
                    ;; See https://www.nurkiewicz.com/2012/04/quartz-scheduler-misfire-instructions.html
                    (cron/with-misfire-handling-instruction-fire-and-proceed))))]
    (task/schedule-task! job trigger)))
