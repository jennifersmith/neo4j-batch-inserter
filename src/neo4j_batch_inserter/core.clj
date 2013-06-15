(ns neo4j-batch-inserter.core
  (:require [neo4j-batch-inserter.interface :refer :all]
            [clojure.tools.logging :as log]))

;;========

(defn inc-results [batch-results key]
  (swap! batch-results #(merge-with + % {key 1})))

(defn run-batch [store-dir operations]
  (with-open [batch-inserter (create-batch-inserter store-dir)]
    (doseq [operation operations]
      (operation batch-inserter))))

(defn insert-node-operation [batch-results node]
  (fn [context]
    (let [ node-id (insert-node context node)]
      (inc-results batch-results :nodes-inserted)
       [node-id node])))

;; I am sure there is a better pattern for this shiz


(defn get-or-create-node-operation [lookup-fn create-fn]
  #(or (lookup-fn %) (create-fn %)))

(defn insert-relationship-operation [batch-results from-node-lookup to-node-lookup properties type]
  (fn [context] 
    (let [from-node (from-node-lookup context) to-node (to-node-lookup context)
          result       (insert-relationship context from-node to-node properties type)]
      (inc-results batch-results :relationships-inserted)
      result)))

(defn create-node-map [{:keys [id-fn type-fn] :or {type-fn :type id-fn :id}} nodes] 
 (zipmap (map id-fn nodes) (map type-fn nodes)))

(defn index-node-operation [batch-results {:keys [type-fn id-fn] :or {type-fn :type id-fn :id}} node-map node-id-fn]
  (fn [context] (let [[node-id properties] (node-id-fn context)
                      external-id (id-fn properties)
                      index (get-index context (type-fn properties) {:type :exact})
                      result (add-to-index index node-id {:id external-id})]
                  (swap! node-map #(assoc % external-id node-id))
                  (inc-results batch-results :nodes-indexed)
                  result)))

(defn lookup-node-operation [ batch-results {:keys [id-fn type-fn] :or {type-fn :type id-fn :id}} node-map properties]
  #(let [
         index (get-index % (type-fn properties) {:type :exact})]
     (inc-results batch-results :nodes-looked-up)
     (or 
      (@node-map (id-fn properties))
      (read-value index :id (id-fn properties)))))

(defn node-validity [{:keys [id-fn type-fn] :or {type-fn :type id-fn :id}} node]
  (cond (nil? (type-fn node)) :invalid
        (nil? (id-fn node)) :invalid
        :else :valid))

(defn relationship-validity [autoindexing {:keys [to from] :as relationship}]
  (cond (nil? (:type relationship)) :invalid
        :else :valid))

;;==== yes another layer====

(defn insert-batch [store-dir {:keys [auto-indexing] }
                    {:keys [nodes relationships] :or {:nodes [] :relationships []}}]
  (let [
        {valid-nodes :valid invalid-nodes :invalid} (group-by #(node-validity auto-indexing %) nodes)
        {valid-relationships :valid invalid-relationships :invalid} (group-by #(relationship-validity auto-indexing %) relationships)
        ; This is so dodgy - need to maange state better
        node-map (atom {})
        batch-results (atom {})
        node-fn #(get-or-create-node-operation 
                  (lookup-node-operation batch-results auto-indexing node-map %
                                         ) 
                  (index-node-operation batch-results auto-indexing node-map (insert-node-operation batch-results %)))
        rel-fn (fn [{:keys [properties from to type]}] (insert-relationship-operation batch-results (node-fn from) (node-fn to) properties type))
        node-operations 
        (map node-fn valid-nodes)
        relationship-operations (map rel-fn valid-relationships)]
    ;; todo: this is dreadful - merge both these functions together and rethink
    (run-batch store-dir (concat node-operations relationship-operations))
(assoc @batch-results :invalid-nodes (count invalid-nodes) :invalid-relationships (count invalid-relationships))))

