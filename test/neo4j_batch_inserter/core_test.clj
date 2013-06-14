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

(defn run-batch [options data]
     (core/insert-batch @neo-dir options data))

(defn run-and-return-db [options data]
  (println
   "Batch insert results"
   (run-batch options data))
  (swap! neo-db (constantly (neo-inspector @neo-dir)))
  @neo-db)


(with-state-changes [
                     (before :facts (create-neo-dir))
                     (after :facts (kill-neo-db)) ]
  (fact "inserts a node with given properties"
        (->
         (run-and-return-db
          {:auto-indexing {:type-fn (constantly "foo") :id-fn :bah}}
          {:nodes [{:bah "blah" :foo "bar"}]})
         (fetch-nodes))
         => (contains {:bah "blah" :foo "bar"}))

  (fact "autoindexes the node with the specifier we give"
        (->
         (run-and-return-db
          {}
          {:nodes [{:type "sock" :id "green"}]})
         (fetch-from-index "sock" "id:green"))
        => [{:type "sock" :id "green"}])

  (fact "able to add a relationship to a newly created node"
    (->
     (run-and-return-db
      {}
      {:relationships [
                       {:from {:id "sock" :type "clothing"} 
                        :to {:id "foot" :type "bodypart"} 
                        :type :goes-on 
                        :properties { :validity "awesome"}}]})
     (fetch-relationships))
    => (contains {:from {:id "sock" :type "clothing"} 
                  :to {:id "foot" :type "bodypart"} 
                  :type :goes-on 
                  :properties { :validity "awesome"}}))
    (fact "able to add a relationship to a node created as part of the payload"
    (->
     (run-and-return-db
      {:auto-indexing {:type-fn (constantly "default") :id-fn :id}}
      {:nodes [{:id "sock" :color "blue"}] :relationships [{:from {:id "sock"} :to {:id "foot"} :type :goes-on :properties { :validity "awesome"}}]})
     (fetch-relationships))
    => (contains {:from {:id "sock" :color "blue"} :to {:id "foot"} :type :goes-on :properties { :validity "awesome"}}))
    (fact "Flags errors for nodes that evaluate to nil types and ids"
      (run-batch {} {:nodes [{:hello 1}]}) => {:invalid-nodes 1})
    (fact "Flags errors for relationships that evaluate to nil types"
      (+ 1 2) => 44))

