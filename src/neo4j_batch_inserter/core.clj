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

(defprotocol Indexer
  (get-index [this index-name index-properties]))

(defprotocol Index
  (set-cache-capacity [this field-name size])
  (add [this node-id properties]))

(defrecord LuceneIndex [index]
  Index
  (add [this node-id properties]
    (.add index node-id (util/create-hashmap properties)))
  (set-cache-capacity [this field-name size]
    (.setCacheCapacity index (util/neo-friendly-key field-name) size)))

(defrecord BatchInserterWrapper [inserter index-inserter]
   NodeInserter
   (insert-node [this node]
     (.createNode inserter (util/create-hashmap node)))
  Indexer
  (get-index [this index-name index-properties]
    (new LuceneIndex
         (.nodeIndex index-inserter
                     (util/neo-friendly-key index-name)
                     (util/create-hashmap index-properties))))
  Closeable
  (close [this]
    (try
      (.shutdown index-inserter)
      (finally
        (.shutdown inserter)))))

(defn create-batch-inserter [path]
  (let [inserter
        (BatchInserters/inserter path)
        index-inserter (new LuceneBatchInserterIndexProvider inserter)
        insert-node (fn [& foo])]
    (new BatchInserterWrapper inserter index-inserter)))
