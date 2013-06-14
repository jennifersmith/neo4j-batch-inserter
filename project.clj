(defproject neo4j-batch-inserter "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.neo4j/neo4j "1.9" :scope "test"]
                 [org.clojure/data.csv "0.1.2"]
                 [org.clojure/tools.logging "0.2.3"]
                  [midje "1.5.1"]
                  [me.raynes/fs "1.4.0" :scope "test"]
                  [org.clojure/tools.logging "0.2.6"]]
  :jvm-opts ["-Xmx2g"])
