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


(defn get-or-create-node-operation [lookup-fn create-fn]
  #(or (lookup-fn %) (create-fn %)))

(defn insert-relationship-operation [from-node-lookup to-node-lookup properties type]
  (fn [context] 
    (let [from-node (from-node-lookup context) to-node (to-node-lookup context)]
      (insert-relationship context from-node to-node properties type))))



(defn create-node-map [{:keys [id-fn type-fn]} nodes] 
 (zipmap (map id-fn nodes) (map type-fn nodes)))

(defn index-node-operation [{:keys [type-fn id-fn]} node-map node-id-fn]
  (fn [context] (let [[node-id properties] (node-id-fn context)
                      external-id (id-fn properties)
                      index (get-index context (type-fn properties) {:type :exact})]
                  (swap! node-map #(assoc % external-id node-id))

                  (add-to-index index node-id {:id external-id}))))

(defn lookup-node-operation [{:keys [id-fn type-fn]} node-map properties]
  #(let [
         index (get-index % (type-fn properties) {:type :exact})]
     (or 
      (@node-map (id-fn properties))
      (read-value index :id (id-fn properties)))))
;;==== yes another layer====

(defn insert-batch [store-dir {:keys [auto-indexing]}
                    {:keys [nodes relationships] :or {:nodes [] :relationships []}}]
  (let [
        ; This is so dodgy - need to maange state better
        node-map (atom {})
        node-fn #(get-or-create-node-operation 
                 
                  (lookup-node-operation auto-indexing node-map %) 
                  (index-node-operation auto-indexing node-map (insert-node-operation %)))
        rel-fn (fn [{:keys [properties from to type]}] (insert-relationship-operation (node-fn from) (node-fn to) properties type))
        node-operations 
        (map node-fn nodes)
        relationship-operations (map rel-fn relationships)]
    (run-batch store-dir (concat node-operations relationship-operations))))

