(ns neo4j-batch-inserter.core
  (:require [neo4j-batch-inserter.util :as util])
  (:import
   (java.io Closeable)
   (org.neo4j.graphdb DynamicRelationshipType)
   (org.neo4j.unsafe.batchinsert BatchInserters
                                 BatchInserter)
   (org.neo4j.index.lucene.unsafe.batchinsert LuceneBatchInserterIndexProvider)))

(defprotocol NodeInserter
  (insert-node [this node]))

(defrecord BatchInserterWrapper [inserter index-inserter]
   NodeInserter
  (insert-node [this node]
    (.createNode inserter (util/create-hashmap node)))
  Closeable
  (close [this]
    (try
      (.shutdown index-inserter)
      (finally
        (.shutdown inserter)))))


(defn get-index [index-inserter index-name]
  (.nodeIndex index-inserter
              index-name
              (util/create-hashmap {"type" "exact"})))



(defn create-node [{:keys [inserter index-inserter]} node]
  (fn [s]
    (let [new-id
          (.createNode inserter (util/create-hashmap node))]
      [new-id (assoc s node new-id)])))


(defn report-on-results [& foo] :ok)

(defn create-batch-inserter [path]
  (let [inserter
        (BatchInserters/inserter path)
        index-inserter (new LuceneBatchInserterIndexProvider inserter)
        insert-node (fn [& foo])]
    (new BatchInserterWrapper inserter index-inserter)))
