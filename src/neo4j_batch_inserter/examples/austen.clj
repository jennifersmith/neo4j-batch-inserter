(ns neo4j-batch-inserter.examples.austen
  (:use [neo4j-batch-inserter.core] [clojure.algo.monads]))

(defn read-characters []
  [
   {:name "Elizabeth Bennet"}])

(defn add-node-m [node]
  (fn [s]
    (println "I added a node")
    [node
     (assoc s node (rand-int 100000))]))

(defn index-node-m [node]
  (fn [s]
    (println "I indexed a node" (get s node))
    [node s]))


(defn add-and-index [node]
  (with-monad state-m
    (domonad [add (add-node-m node)
              state2 (index-node-m node)]
              state2)))


(defn import-data [database-path]
  (with-open  [inserter (create-batch-inserter database-path)]
    (let [
          characters (get-index inserter :characters {:type :exact})
          new-node-id     (insert-node inserter (first (read-characters)))]
      (add characters new-node-id (first (read-characters))))))