(ns lotuc.akka.common.slf4j)

(set! *warn-on-reflection* true)

(defmacro slf4j-log
  "org.slf4j.Logger"
  [logger n format-string & [arg0 arg1 :as args]]
  (let [n'      (symbol (str "." (name n)))
        logger' (vary-meta (gensym "log-")    assoc :tag 'org.slf4j.Logger)
        format' (vary-meta (gensym "format-") assoc :tag `String)
        arg0'   (vary-meta (gensym "arg0-")   assoc :tag `Object)
        arg1'   (vary-meta (gensym "arg1-")   assoc :tag `Object)
        arr     (vary-meta (gensym "args-")   assoc :tag "[Ljava.lang.Object;")]
    ;; https://www.slf4j.org/api/org/slf4j/Logger.html
    `(let [~logger' ~logger
           ~format' ~format-string]
       ~(condp = (count args)
          0 `(~n' ~logger' ~format')
          1 `(let [~arg0' (str ~arg0)]
               (~n' ~logger' ~format' ~arg0'))
          2 `(let [~arg0' (str ~arg0)
                   ~arg1' (str ~arg1)]
               (~n' ~logger' ~format' ~arg0' ~arg1'))
          `(let [~arr (into-array Object [~@args])]
             (~n' ~logger' ~format' ~arr))))))

(defmacro ->LogLevel [level]
  (if-some [v (get {:error 'org.slf4j.event.Level/ERROR
                    :warn  'org.slf4j.event.Level/WARN
                    :info  'org.slf4j.event.Level/INFO
                    :debug 'org.slf4j.event.Level/DEBUG
                    :trace 'org.slf4j.event.Level/TRACE}
                   (when (keyword? level) level))]
    v
    `(let [v# ~level]
       (or (get {:error org.slf4j.event.Level/ERROR
                 :warn  org.slf4j.event.Level/WARN
                 :info  org.slf4j.event.Level/INFO
                 :debug org.slf4j.event.Level/DEBUG
                 :trace org.slf4j.event.Level/TRACE}
                (when (keyword? v#) v#))
           (and (instance? org.slf4j.event.Level v#) v#)
           (throw (ex-info (str "illegal slf4j log level: " v#) {:value v#}))))))

(comment
  (macroexpand '(slf4j-log logger info "hello"))
  (macroexpand '(slf4j-log logger info "hello {}"       "1"))
  (macroexpand '(slf4j-log logger info "hello {} {}"    "1" "2"))
  (macroexpand '(slf4j-log logger info "hello {} {} {}" "1" "2" "3"))

  ;; expand a compile time value
  (macroexpand '(let [level :info] (->LogLevel level)))
  ;; expand a runtime time value
  (macroexpand '(->LogLevel level))
  (let [level :info] (->LogLevel level)))
