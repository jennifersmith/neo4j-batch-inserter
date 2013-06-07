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

(defn run-and-return-db [options data]
  (core/insert-batch @neo-dir options data)
  (swap! neo-db (constantly (neo-inspector @neo-dir)))
  @neo-db)


(with-state-changes [
                     (before :facts (create-neo-dir))
                     (after :facts (kill-neo-db)) ]
  (fact "inserts a node with given properties"
        (->
         (run-and-return-db
          {:auto-indexing {:type-fn (constantly "foo") :id-fn :bah}}
          {:nodes [{:bah "blah" }]})
         (fetch-nodes))
         => (contains {:bah "blah"}))

  (fact "autoindexes the node with the specifier we give"
        (->
         (run-and-return-db
          {:auto-indexing {:type-fn :type :id-fn :id}}
          {:nodes [{:type "sock" :id "green"}]})
         (fetch-from-index "sock" "id:green"))
        => [{:type "sock" :id "green"}])
  (fact "able to add a relationship to a newly created node"
    (->
     (run-and-return-db
      {:auto-indexing {:type-fn (constantly "default") :id-fn :id}
       :relationships {:type-fn :type}}
      {:relationships [{:from {:id "sock"} :to {:id "foot"} :type :goes-on :properties { :validity "awesome"}}]})
     (fetch-relationships))
    => (contains {:from {:id "sock"} :to {:id "foot"} :type :goes-on :properties { :validity "awesome"}}))
    (fact "able to add a relationship to a node created as part of the payload"
    (->
     (run-and-return-db
      {:auto-indexing {:type-fn (constantly "default") :id-fn :id}
       :relationships {:type-fn :type}}
      {:nodes [{:id "sock" :color "blue"}] :relationships [{:from {:id "sock"} :to {:id "foot"} :type :goes-on :properties { :validity "awesome"}}]})
     (fetch-relationships))
    => (contains {:from {:id "sock" :color "blue"} :to {:id "foot"} :type :goes-on :properties { :validity "awesome"}})))
