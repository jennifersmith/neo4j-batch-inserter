(ns neo4j-batch-inserter.core
  (:require [neo4j-batch-inserter.interface :refer :all]
            [clojure.tools.logging :as log]))

;;util

;; needed?
(defn merge-meta [obj m]
  (with-meta obj (merge (meta obj) m)))
;;========

(defn inc-results [batch-results key]
  (swap! batch-results #(merge-with + % {key 1})))




(defn add-identity-properties
  [{:keys [id-fn type-fn] :or {type-fn :type id-fn :id}} node]
  (merge-meta node {::identity {:id (id-fn node) :type (type-fn node)}}))

(defn identity-property-fn [property]
  #(-> %
       (meta)
       ::identity
       property))

(def identity-type (identity-property-fn :type))
(def identity-id (identity-property-fn :id))

;; ==== "operations" ?

(defn insert-node-operation [ node]
  (fn [{:keys [batch-inserter batch-results]}]
    (let [ node-id (insert-node batch-inserter node)]
      (inc-results batch-results :nodes-inserted)
       [node-id node])))

(defn get-or-create-node-operation [lookup-fn create-fn]
  #(or (lookup-fn %) (create-fn %)))

(defn insert-relationship-operation [from-node-lookup to-node-lookup properties type]
  (fn [{:keys [batch-results batch-inserter] :as context}]
    (let [from-node (from-node-lookup context) to-node (to-node-lookup context)
          result (insert-relationship batch-inserter from-node to-node properties type)]
      (inc-results batch-results :relationships-inserted)
      result)))

(defn index-node-operation [node-id-fn]
  (fn [{:keys [batch-inserter batch-results node-map] :as context}] 
    (let [[node-id properties] (node-id-fn context)
                      external-id (identity-id properties)
                      index (get-index batch-inserter (identity-type properties) {:type :exact})
                      result (add-to-index index node-id {:id external-id})]
                  (swap! node-map #(assoc % external-id node-id))
                  (inc-results batch-results :nodes-indexed)
                  result)))

(defn lookup-node-operation [properties]
  (fn [{:keys [batch-inserter batch-results node-map]}]
    (let [
          index (get-index batch-inserter
                           (identity-type properties)
                           {:type :exact})
          id (identity-id properties)]
      (inc-results batch-results :nodes-looked-up)
      (or
       (@node-map id)
       (read-value index :id id)))))

(defn node-validity [node]
  (cond (nil? (identity-id node)) :invalid
        (nil? (identity-type node)) :invalid
        :else :valid))

(defn relationship-validity [{:keys [to from] :as relationship}]
  (cond (nil? (:type relationship)) :invalid
        :else :valid))

;; yauch
(defn if-valid-node-operation [node next-fn]
  (fn [{:keys [batch-results] :as context}]
    (if (= :valid (node-validity node))
      (next-fn context)
      (do
        (inc-results batch-results :invalid-nodes)
        nil))))

(defn if-valid-rel-operation [rel next-fn]
  (fn [{:keys [batch-results] :as context}]
    (if (= :valid (relationship-validity rel))
      (next-fn context)
      (do
        (inc-results batch-results :invalid-relationships)
        nil))))

;; XXX:  note that we are adding identity properties at operation-create time
;; not at execution time...
;; need to do everything at one time or another not both cos it confusing

(defn create-massive-function-thing-for-nodes
  [auto-indexing]
  (fn [node] (let [node (add-identity-properties auto-indexing node)]
               (if-valid-node-operation node
                                        (get-or-create-node-operation
                                         (lookup-node-operation node)
                                         (index-node-operation
                                          (insert-node-operation node)))))))

(defn create-massive-function-thing-for-rels
  [node-fn]
  (fn [{:keys [properties from to type] :as rel}]
    (if-valid-rel-operation
     rel
     (insert-relationship-operation (node-fn from)
                                    (node-fn to)
                                    properties type))))

;;==== yes another layer====

(defn insert-batch [store-dir {:keys [auto-indexing] }
                    {:keys [nodes relationships] :or {:nodes [] :relationships []}}]
  (let [
        node-fn (create-massive-function-thing-for-nodes auto-indexing)
        rel-fn (create-massive-function-thing-for-rels node-fn)
        node-operations (map node-fn nodes)
        relationship-operations (map rel-fn relationships)]
    ;; todo: this is dreadful - merge both these functions together and rethink
    (with-open [batch-inserter (create-batch-inserter store-dir)]
      (let [context {:batch-inserter batch-inserter
                     :node-map (atom {})
                     :batch-results (atom {})}]
        (doseq [operation (concat node-operations relationship-operations)]
          (operation context))
        @(context :batch-results)))))
