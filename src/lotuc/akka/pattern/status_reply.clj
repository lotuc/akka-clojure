(ns lotuc.akka.pattern.status-reply
  (:import
   (akka.pattern StatusReply)))

(defmacro status-reply-of [& body]
  `(try (StatusReply/success (do ~@body))
        (catch Throwable t# (StatusReply/error t#))))

(comment
  (.isSuccess (status-reply-of 1))
  (.getValue (status-reply-of 1))
  (.isSuccess (status-reply-of (/ 1 0)))
  (.getValue (status-reply-of (/ 1 0))))
