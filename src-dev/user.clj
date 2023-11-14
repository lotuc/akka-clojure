(ns user
  (:require
   [lambdaisland.classpath.watch-deps :as watch-deps]))

(future (watch-deps/start! {}))
