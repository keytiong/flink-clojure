# flink-clojure

Clojure wrapper for Apache Flink

## Usage

### Leiningen

```clojure
[io.kosong.flink/flink-clojure "0.1.0-SNAPSHOT"]
```

## Example

```clojure
(require '[io.kosong.flink.clojure.core :as fk])

(import 'org.apache.flink.streaming.api.environment.StreamExecutionEnvironment)

(def env (StreamExecutionEnvironment/getExecutionEnvironment))

(fk/register-clojure-types env)

(def word-count-data
  ["To be, or not to be,--that is the question:--"
   "Whether 'tis nobler in the mind to suffer"
   ;...
   ])

(def tokenizer
  (fk/flink-fn
    {:fn      :flat-map
     :returns (fk/type-info-of [])
     :flatMap (fn [this line collector]
                (doseq [word (-> line .toLowerCase (.split "\\W+"))]
                  (.collect collector [word 1])))}))

(def counter
  (fk/flink-fn
    {:fn      :reduce
     :returns (fk/type-info-of [])
     :reduce  (fn [this [word-1 count-1] [word-2 count-2]]
                [word-1 (+ count-1 count-2)])}))

(def word-selector
  (fk/flink-fn
    {:fn      :key-selector
     :returns (fk/type-info-of String)
     :getKey  (fn [this [word count]]
                word)}))

(-> env
  (.fromCollection word-count-data)
  (.flatMap tokenizer)
  (.keyBy word-selector)
  (.reduce counter)
  (.print))

(.execute env "Word Count")
```

## Build

```shell
lein clean
lein install
```