(ns neo4j-batch-inserter.inspector
  (:use neo4j-batch-inserter.util)
  (:import
   (org.neo4j.kernel EmbeddedGraphDatabase)))

(defn neo-inspector [store-dir]
  (new EmbeddedGraphDatabase store-dir)
  )

(defn close [neo-inspector]
  (.shutdown neo-inspector))

(defn node-to-map [node]
  {:bah "Baz"})

(defn fetch-nodes [inspector]
  (map node-to-map
       (.getAllNodes inspector))
)