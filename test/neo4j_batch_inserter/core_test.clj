(ns neo4j-batch-inserter.core-test
  (:use midje.sweet)
  (:use neo4j-batch-inserter.inspector)
  (:use [me.raynes.fs :only [temp-dir]])
  (:require [neo4j-batch-inserter.core :as core]))

(def neo-dir (atom nil))
(def neo-db (atom nil))

(defn create-neo-dir []
  (swap! neo-dir (constantly (.getAbsolutePath (temp-dir "neo4j-batch-inserter"))))
  (println "Creating Neo4j Store directory: " @neo-dir ))

(defn kill-neo-db []
  (if @neo-db
      (close @neo-db)))

(defn run-and-return-db [data]
  (core/insert-batch @neo-dir data)
  (swap! neo-db (constantly (neo-inspector @neo-dir)))
  @neo-db)

(with-state-changes [
                     (before :facts (create-neo-dir))
                     (after :facts (kill-neo-db)) ]
  (fact "inserts a node with given properties"
        (->
         (run-and-return-db
          {:nodes [{:bah "blah"}]})
         (fetch-nodes))
         => (contains {:bah "blah"})))
