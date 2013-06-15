(ns neo4j-batch-inserter.examples.austen
  (:use [neo4j-batch-inserter.core]))

(defn fetch-characters []
  (map #(assoc {:type "character"} :name %1 :id %2)
       ["Jane Bennet"
         "Elizabeth Bennet"
         "Lydia Bennet"
         "Charles Bingley"
         "George Wickham"
         "Fitzwilliam Darcy"] 
       [:jb :lb :cb :gw :fd]))

(defn make-association [from type to]
  {:from {:id from :type :character} :to {:id to :type :character} :type type})

(defn fetch-associations []
  [(make-association :jb :sisterOf :lb)
   (make-association :jb :loves :cb)
   (make-association :eb :fallsInLoveWith :fd)
   (make-association :eb :fancies :gw)
   (make-association :eb :sisterOf :lb)
   (make-association :cb :loves :jb)
   (make-association  :cb :friendsWith :fd)
   (make-association :fd :fallsInLoveWith :eb)
   (make-association :fd :friendsWith :cb)])


(defn -main [database-path]
  (println "Creating austen graph at: " database-path)
  (insert-batch 
   database-path 
   {:auto-indexing {:type-fn :type :id-fn :id}}
   {:nodes (fetch-characters) :relationships (fetch-associations)}))

