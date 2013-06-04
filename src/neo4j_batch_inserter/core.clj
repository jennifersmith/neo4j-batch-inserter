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
  (insert-relationship [this from-node to-node properties type]))

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
   (insert-relationship [this from-node to-node properties type]
     (let [rel-type (DynamicRelationshipType/withName (util/neo-friendly-key type))]
       (.createRelationship inserter from-node to-node rel-type (util/create-hashmap properties) ))
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

(defn add-to-index [index node-id properties]
  (.add index node-id (util/create-hashmap properties))
  node-id)

;;========

(defn run-batch [store-dir operations]
  (with-open [batch-inserter (create-batch-inserter store-dir)]
    (doseq [operation operations]
      (operation batch-inserter))))

(defn insert-node-operation [properties]
  #(let [ node-id ( insert-node % properties)]
     [node-id properties]))

;; I am sure there is a better pattern for this shiz

(defn index-node-operation [{:keys [type-fn id-fn]} node-id-fn]
  #(let [[node-id properties] (node-id-fn %)
         external-id (id-fn properties)
         index (get-index % (type-fn properties) {:type :exact})]
     (println node-id " here")
     (add-to-index index node-id {:id external-id})))

(defn insert-relationship-operation [from-node-lookup to-node-lookup properties type]
  (fn [context] 
    (let [from-node (from-node-lookup context) to-node (to-node-lookup context)]
      (println from-node-lookup)
      (insert-relationship context from-node to-node properties type))))

;;==== yes another layer====

(defn insert-batch [store-dir {:keys [auto-indexing]}
                    {:keys [nodes relationships] :or {:nodes [] :relationships []}}]
  (let [
        node-fn (if (nil? auto-indexing)
                  insert-node-operation
                  (comp #(index-node-operation auto-indexing %) insert-node-operation ))
        rel-fn (fn [{:keys [properties from to type]}] (insert-relationship-operation (node-fn from) (node-fn to) properties type))
        node-operations 
        (map node-fn nodes)
        relationship-operations (map rel-fn relationships)]
    (run-batch store-dir (concat node-operations relationship-operations))))

