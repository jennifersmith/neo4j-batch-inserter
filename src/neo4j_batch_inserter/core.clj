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

(defn identity-meta [property value]
  (-> value
       (meta)
       ::identity
       property))

(def identity-type (partial identity-meta :type))
(def identity-id (partial identity-meta :id))

;; ==== "operations" ?

(defn insert-node-operation [ node {:keys [batch-inserter batch-results]}]
  (let [ node-id (insert-node batch-inserter node)]
    (inc-results batch-results :nodes-inserted)
    [node-id node]))

(defn get-or-create-node-operation [lookup-fn create-fn context]
  (or (lookup-fn context) (create-fn context)))

(defn insert-relationship-operation [from-node-lookup to-node-lookup properties type {:keys [batch-results batch-inserter] :as context}]
  (let [from-node (from-node-lookup context) to-node (to-node-lookup context)
        result (insert-relationship batch-inserter from-node to-node properties type)]
    (inc-results batch-results :relationships-inserted)
    result))

(defn index-node-operation [[node-id properties] {:keys [batch-inserter batch-results node-map] :as context}]
  (let [ external-id (identity-id properties)
        index (get-index batch-inserter (identity-type properties) {:type :exact})
        result (add-to-index index node-id {:id external-id})]
    (swap! node-map #(assoc % external-id node-id))
    (inc-results batch-results :nodes-indexed)
    result))

(defn lookup-node-operation [properties {:keys [batch-inserter batch-results node-map]}]
  (let [
        index (get-index batch-inserter
                         (identity-type properties)
                         {:type :exact})
        id (identity-id properties)]
    (inc-results batch-results :nodes-looked-up)
    (or
     (@node-map id)
     (read-value index :id id))))

(defn node-validity [node]
  (cond (nil? (identity-id node)) :invalid
        (nil? (identity-type node)) :invalid
        :else :valid))

(defn relationship-validity [{:keys [to from] :as relationship}]
  (cond (nil? (:type relationship)) :invalid
        :else :valid))

;; yauch
(defn if-valid-node-operation [node {:keys [batch-results] :as context}]
  (if (= :valid (node-validity node))
    true
    (do
      (inc-results batch-results :invalid-nodes)
      false)))

(defn if-valid-rel-operation [rel {:keys [batch-results] :as context}]
  (if (= :valid (relationship-validity rel))
    true
    (do
      (inc-results batch-results :invalid-relationships)
      false)))

;; XXX:  note that we are adding identity properties at operation-create time
;; not at execution time...
;; need to do everything at one time or another not both cos it confusing

(defn create-massive-function-thing-for-nodes
  [auto-indexing]
  (fn [node context]
    (let [node (add-identity-properties auto-indexing node)]
      (if (if-valid-node-operation node context)
        (or
         (lookup-node-operation node context)
         (index-node-operation
          (insert-node-operation node context) context))))))

(defn create-massive-function-thing-for-rels
  [node-fn]
  (fn [{:keys [properties from to type] :as rel} context]
    (if (if-valid-rel-operation rel context)
     (insert-relationship-operation (partial node-fn from)
                                    (partial node-fn to)
                                    properties type context))))

;;==== yes another layer====

(defn insert-batch [store-dir {:keys [auto-indexing] }
                    {:keys [nodes relationships] :or {:nodes [] :relationships []}}]
  (let [
        node-fn (create-massive-function-thing-for-nodes auto-indexing)
        rel-fn (create-massive-function-thing-for-rels node-fn)
        relationship-operations (map rel-fn relationships)]
    ;; todo: this is dreadful - merge both these functions together and rethink
    (with-open [batch-inserter (create-batch-inserter store-dir)]
      (let [context {:batch-inserter batch-inserter
                     :node-map (atom {})
                     :batch-results (atom {})}]
        (doseq [node nodes]
          (node-fn node context))
        (doseq [relationship relationships]
          (rel-fn relationship context))
        @(context :batch-results)))))

(comment
(insert-batch "/tmp/fugit" {} {:relationships [{
                                                :from {:type "princess" :id 1}
                                                :to {:type "frog" :id 2}
                                                :type "kissed"}]}))
