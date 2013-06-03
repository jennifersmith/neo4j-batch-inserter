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
  (let [keys (.getPropertyKeys node) values (map #(.getProperty node %) keys)]
    (zipmap (map clojure-friendly-key keys) values)))

(defn fetch-nodes [inspector]
  (map node-to-map
       (.getAllNodes inspector)))

(defn fetch-from-index [inspector index query]
  (let [index
        (.. inspector index (forNodes index))]
    (map node-to-map (iterator-seq
                      (.query index query)))))
