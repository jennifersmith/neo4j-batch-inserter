(ns neo4j-batch-inserter.core
  (:require [neo4j-batch-inserter.util :as util])
  (:import
   (java.io Closeable)
   (org.neo4j.graphdb DynamicRelationshipType )
   (org.neo4j.kernel EmbeddedGraphDatabase)
   (org.neo4j.unsafe.batchinsert BatchInserters
                                 BatchInserter)
   (org.neo4j.index.lucene.unsafe.batchinsert LuceneBatchInserterIndexProvider)))

(defprotocol NodeInserter
  (insert-node [this node]))

(defprotocol RelationshipInserter
  (insert-relationship [this from-node to-node properties]))

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
   RelationshipInserter
   (insert-relationship [this from-node to-node {:keys [type properties] :or {:properties {}}}]
     (let [rel-type (DynamicRelationshipType/withName type)]
       (.createRelationship from-node to-node type (util/create-hashmap properties)  ))
     )
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
        index-inserter (new LuceneBatchInserterIndexProvider inserter)]
    (new BatchInserterWrapper inserter index-inserter)))

;;========

(defn run-batch [store-dir operations]
  (with-open [batch-inserter (create-batch-inserter store-dir)]
    (doseq [operation operations]
      (operation batch-inserter))))

(defn insert-node-operation [properties]
  #(insert-node % properties))


(defn insert-relationship-operation [properties from-node-lookup to-node-lookup]
  (fn [context ] 
    (let [from-node (from-node-lookup context) to-node (to-node-lookup context)]
      (insert-relationship context from-node to-node properties))))
