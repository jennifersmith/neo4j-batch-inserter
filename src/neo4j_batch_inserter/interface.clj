(ns neo4j-batch-inserter.interface
  (:require [ clojure.string :as string] 
            [neo4j-batch-inserter.util :as util])
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
  (insert-relationship [this from-node to-node properties type]))

(defprotocol Indexer
  (get-index [this index-name index-properties]))

(defprotocol Index
  (set-cache-capacity [this field-name size])
  (add [this node-id properties])
  (read-value [this key value]))

(defrecord LuceneIndex [index]
  Index
  (add [this node-id properties]
    (.add index node-id (util/create-hashmap properties)))
  (set-cache-capacity [this field-name size]
    (.setCacheCapacity index (util/neo-friendly-key field-name) size))
  (read-value [this key value] 
    (.. index 
        (get (util/neo-friendly-key key) (cast Object value))
        (getSingle))))

(defrecord BatchInserterWrapper [inserter index-inserter]
   NodeInserter
   (insert-node [this node]
     (.createNode inserter (util/create-hashmap node))) ;; TODO: Use labels!
   RelationshipInserter
   (insert-relationship [this from-node to-node properties type]
     (let [rel-type (DynamicRelationshipType/withName (util/neo-friendly-key type))]
       (.createRelationship inserter from-node to-node rel-type (util/create-hashmap properties) )))
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

(defn add-to-index [index node-id properties]
  (.add index node-id (util/create-hashmap properties))
  node-id)
