(ns neo4j-batch-inserter.interface
  (:require [ clojure.string :as string])
  (:import (org.neo4j.graphdb DynamicRelationshipType)
           (org.neo4j.unsafe.batchinsert BatchInserters
                                         BatchInserter)
           (org.neo4j.index.lucene.unsafe.batchinsert LuceneBatchInserterIndexProvider)))

(defn- lower-camel [s]
  (let [[first & remaining] (string/split s #"-")]
    (apply str (cons first (map string/capitalize remaining)))))

(defn- neo-friendly-key [key]
  (-> key
      (name)
      (lower-camel)))

(defn- camel-to-dash
  [s]
  (apply str (map #(if (Character/isUpperCase %)
                     (str "-" (clojure.string/lower-case %))
                     %)
                  s)))

(defn- clojure-friendly-key [key]
  (keyword (camel-to-dash key)))

(defn- is-neo-friendly? [val]
  (or (number? val) (string? val) (= java.lang.Boolean (class val))))

(defn- neo-friendly-val [val]
  (cond (is-neo-friendly? val) val
      (or (symbol? val) (keyword? val)) (name val)
      :else (str val)))

(defn- create-hashmap [m]
  (new java.util.HashMap
       (zipmap (map neo-friendly-key (keys m)) (map neo-friendly-val (vals m)))))


(defn- dynamic-relationship [name]
  (DynamicRelationshipType/withName (neo-friendly-key name)))

(defn insert-relationship [{:keys [inserter]} {:keys [from-node to-node type]}]
  (.createRelationship inserter from-node to-node (dynamic-relationship type) nil))

(defn get-index [{:keys [index-inserter]} node-type]
  (.nodeIndex index-inserter
              (neo-friendly-key node-type)
              (create-hashmap {"type" "exact"})))

(defn set-cache-capacity [index field-name size]
  (.setCacheCapacity index (neo-friendly-key field-name) size))

(defn add-to-index [index node-id properties]
    (.add index node-id (create-hashmap properties)))


(defn lookup-entity-foo [index field-name field-value]
  (-> index
      (.get (neo-friendly-key field-name) field-value)
      (iterator-seq)))

(defn lookup-entity [index field-name field-value]
  (-> index
      (.get (neo-friendly-key field-name) field-value)
      (.getSingle)))

(defn create-batch-inserter [path]
  (let [inserter
        (BatchInserters/inserter path)
        index-inserter (new LuceneBatchInserterIndexProvider inserter)]
    {:inserter inserter :index-inserter index-inserter}))

(defn create-node [{:keys [inserter index-inserter]} node]
  (let [new-id
        (.createNode inserter (create-hashmap node))]
    {:node-id new-id :node node}))



(defn- parse-relationship [relationship]
  {:from-node (.getStartNode relationship)
   :to-node (.getEndNode relationship)
   :type (clojure-friendly-key (.name (.getType relationship)))}
  )
(defn get-relationships [{:keys [inserter]} node-id]
  (map parse-relationship (.getRelationships inserter  node-id))
)