(ns neo4j-batch-inserter.inspector
  (:use neo4j-batch-inserter.util)
  (:import
   (org.neo4j.graphdb Direction)
   (org.neo4j.kernel EmbeddedGraphDatabase)))

(defn neo-inspector [store-dir]
  (new EmbeddedGraphDatabase store-dir)
  )

(defn close [neo-inspector]
  (.shutdown neo-inspector))

(defn propertycontainer-to-map [node]
  (let [keys (.getPropertyKeys node) values (map #(.getProperty node %) keys)]
    (zipmap (map clojure-friendly-key keys) values)))

(defn relationship-to-map [relationship]
  { :properties (propertycontainer-to-map relationship)
   :type (clojure-friendly-key (.. relationship getType name))
   :from (node-to-map (.getStartNode relationship))
   :to (node-to-map (.getEndNode relationship))}
  
)
(defn fetch-nodes [inspector]
  (map propertycontainer-to-map
       (.getAllNodes inspector)))

(defn fetch-relationships [inspector]
  (let [nodes (.getAllNodes inspector)
        relationships (mapcat #(.getRelationships % Direction/OUTGOING) nodes)]
    (map relationship-to-map relationships)))

(defn fetch-from-index [inspector index query]
  (let [index
        (.. inspector index (forNodes index))]
    (map propertycontainer-to-map (iterator-seq
                      (.query index query)))))
