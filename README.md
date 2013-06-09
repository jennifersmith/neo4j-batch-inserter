# neo4j-batch-inserter

A clojure library wrapping the neo4j batch API to allow you to quickly insert batches of nodes and relationships into a neo4j database. Pretty opinionated - in that it does what I need it do do. Nodes are auto-indexed.

## TODO

* Fix the group name etc. to something sensible
* Use MapDB or similar for faster lookup of nodes than the lucene index
* Check existence of relationships before inserting?
* Clean up the crappy internal implenentation :) 
* Better examples and docs

## Usage

Usage may change (and the underlying implementation certainly will). Right now, the best method to use is this one:


``` clojure

(:require [neo4j-batch-inserter.core :refer [insert-batch]])

(insert-batch
        "/path/to/new/store"	
	{:auto-indexing {:type-fn (constantly "default") :id-fn :id}
       	{:nodes [{:id "sock" :color "blue"}] 
	 :relationships [{:from {:id "sock"} :to {:id "foot"} :type :goes-on :properties { :validity "awesome"}}]})
```

## License

Copyright © 2013 Jennifer Smith and ThoughtWorks ltd.

Distributed under the [Eclipse Public License v 1.0](http://www.eclipse.org/legal/epl-v10.html).