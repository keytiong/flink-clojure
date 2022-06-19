(defproject io.kosong.flink/flink-clojure "0.2.0"
  :description "Clojure wrapper for Apache Flink"
  :url "https://github.com/keytiong/flink-clojure"
  :license {:name "The MIT License"
            :url  "http://opensource.org/licenses/MIT"}
  :plugins []

  :source-paths ^:replace ["src/main/clojure"]
  :test-paths ["src/test/clojure" "src/test/java"]
  :resource-paths ["src/main/resource"]

  :java-source-paths ["src/main/java"]

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.taoensso/nippy "3.1.1"]
                 [org.apache.logging.log4j/log4j-api "2.17.2"]
                 [com.esotericsoftware.kryo/kryo "2.24.0"]]

  :profiles {:provided
             {:dependencies
              [[org.apache.flink/flink-streaming-java "1.15.0"]]}}
  )
