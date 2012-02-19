(ns clutchbase.client
  (:import [com.couchbase.client
            CouchbaseClient])
  (:import [java.net
            URI])
  (:require [clojure.data.json :as json]))

(defn make-client [uris]
  (CouchbaseClient. (map #(URI. %) uris) "default" ""))

(defn add 
  ([client key expiry value]
    "Add a value with the specified key that does not already exist"
    (.add client key expiry (json/json-str value)))
  ([client key expiry value transcoder]
    "Add a value that does not already exist using custom transcoder"
    (.add client key expiry value transcoder)))

(defn append 
  ([client casunique key value]
    "Append a value to an existing key"
    (.append client casunique key (json/json-str value)))
  ([client casunique key value transcoder]
    "Append a value to an existing key"
    (.append client casunique key value)))
  
(defn async-cas
  ([client key casunique value]
    "Asynchronously compare and set a value"
    (.asyncCAS client key casunique value))	
  ([client key casunique expiry value transcoder]
    "Asynchronously compare and set a value with custom transcoder and expiry"
    (.asyncCAS client key casunique expiry value transcoder))	
  ([client key casunique value transcoder]
    "Asynchronously compare and set a value with custom transcoder"
    (.asyncCAS client key casunique value transcoder)))

(defn async-decr [client key offset]
  "Asynchronously decrement the value of an existing key"
  (.asyncDecr client key offset))

(defn async-get-and-touch
  ([client key expiry]	
    "Asynchronously get a value and update the expiration time for a given key"
    (.asyncGetAndTouch client key expiry))
  ([client key expiry transcoder]	
    "Asynchronously get a value and update the expiration time for a given key using a custom transcoder"
    (.asyncGetAndTouch client key expiry transcoder)))

(defn async-get
  ([client key]
    "Asynchronously get a single key"
    (.asyncGet client key))
  ([client key transcoder]
    "Asynchronously get a single key using a custom transcoder"
    (.asyncGet client key transcoder)))

(defn async-get-bulk [client & {:keys [keyn transcoder keycollection]}]
  "Asynchronously get multiple keys"
  (cond 
    (and keyn transcoder) (.asyncGetBulk client transcoder keyn)
    (and keycollection transcoder) (.asyncGetBulk client keycollection transcoder)
    (keyn) (.asyncGetBulk client keyn)
    (keycollection) (.asyncGetBulk client keycollection)
    :else (throw (Exception. "keyn or keycollection must be set"))))

(defn get-bulk [client & {:keys [keyn transcoder keycollection]}]
  "get multiple keys"
  (cond 
    (and keyn transcoder) (.getBulk client transcoder keyn)
    (and keycollection transcoder) (.getBulk client keycollection transcoder)
    (keyn) (.getBulk client keyn)
    (keycollection) (.getBulk client keycollection)
    :else (throw (Exception. "keyn or keycollection must be set"))))

(defn async-gets
  ([client key]	
    "Asynchronously get single key value with CAS value"
    (.asyncGets client key))
  ([client key transcoder]
    "Asynchronously get single key value with CAS value using custom transcoder"
    (.asyncGets client key transcoder)))

(defn async-incr [client key offset]
  "Asynchronously increment the value of an existing key"
  (.asyncIncr client key offset))

(defn cas 
  ([client key casunique value]
    "Compare and set"
    (.cas client key casunique (json/json-str value)))
  ([client key casunique expiry value transcoder]	
    "Compare and set with a custom transcoder and expiry"
    (.cas client key casunique expiry value transcoder))
  ([client key casunique value transcoder]
    "Compare and set with a custom transcoder"
    (.cas client key casunique value transcoder)))

(defn decr 
  ([client key offset]	
    "Decrement the value of an existing numeric key"
    (.decr client key offset))
  ([client key offset default]
    "Decrement the value of an existing numeric key"
    (.decr client key offset default))
  ([client key offset default expiry]
    "Decrement the value of an existing numeric key"
    (.decr client key offset default expiry)))

(defn delete [client key]	
  "Delete the specified key"
  (.delete client key))

(defn get-and-touch
  ([client key expiry]	
    "Get a value and update the expiration time for a given key"
    (.getAndTouch client key expiry))
  ([client key expiry transcoder]
    "Get a value and update the expiration time for a given key using a custom transcoder"
    (.getAndTouch client key expiry transcoder)))
  
(defn get
  ([client key]	
    "Get a single key"
    (json/read-json 
      (.get client key)))
  ([client key transcoder]
    "Get a single key using a custom transcoder"
    (.get client key transcoder)))

(defn gets
  ([client key]
    "Get single key value with CAS value"
    (json/read-json 
      (.gets client key)))
  ([client key transcoder]
    "Get single key value with CAS value using custom transcoder"
    (.gets client key transcoder)))

(defn get-stats
  ([client]
    "Get the statistics from all connections"
    (.getStats client))
  ([client statname]
    "Get the statistics from all connections"
    (.getStats client statname)))

(defn incr
  ([client key offset]	
    "Increment the value of an existing numeric key"
    (.incr client key offset))
  ([client key offset default]
    "Increment the value of an existing numeric key"
    (.incr client key offset default))
  ([client key offset default expiry]
    "Increment the value of an existing numeric key"
    (.incr client key offset default expiry)))

(defn prepend 
  ([client casunique key value]
    "Prepend a value to an existing key using the default transcoder"
    (.prepend client casunique key (json/json-str value)))
  ([client casunique key value transcoder]
    "Prepend a value to an existing key using a custom transcoder"
    (.prepend client casunique key value transcoder)))

(defn replace 
  ([client key value expiry]
    "Update an existing key with a new value"
    (.replace client key value expiry))
  ([client key value expiry transcoder]
    "Update an existing key with a new value using a custom transcoder"))
    
(defn set
  ([client key expiry value]
    "Store a value using the specified key"
    (.set client key expiry (json/json-str value)))
  ([client key expiry value transcoder]
    "Store a value using the specified key"
    (.set client key expiry value transcoder)))

(defn touch [client key expiry]
  "Update the expiry time of an item"
  (.touch client key expiry))

