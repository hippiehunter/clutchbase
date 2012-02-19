(defproject clutchbase "1.0.0-SNAPSHOT"
  :description "A clojure wrapper for the couchbase java client"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [couchbase/couchbase-client "1.0.1"
                  :exclusions [javax.jms/jms
                               com.sun.jmx/jmxri
                               com.sun.jdmk/jmxtools]]
                 [org.clojure/data.json "0.1.2"]]
  :repositories {"couchbase" "http://files.couchbase.com/maven2"})