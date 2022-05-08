(ns io.kosong.flink.clojure.core
  (:import (io.kosong.flink.clojure NippySerializer CljTimestampAssigner)
           (clojure.lang RT)
           (clojure.lang PersistentVector PersistentHashMap PersistentHashSet PersistentArrayMap PersistentStructMap
                         PersistentTreeMap PersistentTreeSet PersistentList APersistentMap)
           (io.kosong.flink.clojure.functions CljMapFunction CljFlatMapFunction CljKeyedProcessFunction
                                              CljProcessFunction CljSinkFunction CljSourceFunction CljKeySelector
                                              CljReduceFunction CljWindowFunction CljFilterFunction CljCoFlatMapFunction CljProcessWindowFunction CljSimpleReduceFunction)
           (org.apache.flink.api.common.typeinfo TypeInformation)))

(def ^:private clojure-collection-types
  [PersistentList
   PersistentVector PersistentHashSet #_PersistentTreeSet
   PersistentHashMap PersistentArrayMap PersistentStructMap #_PersistentTreeMap])

(defn register-clojure-types [env]
  (let [exec-config (.getConfig env)]
    (doseq [type clojure-collection-types]
      (.registerTypeWithKryoSerializer exec-config type NippySerializer))
    env))

(defn- ensure-namespace [args]
  (if (:ns args)
    args
    (assoc args :ns *ns*)))

(def keyword->fn-class
  {:map            CljMapFunction
   :filter         CljFilterFunction
   :flat-map       CljFlatMapFunction
   :keyed-process  CljKeyedProcessFunction
   :process        CljProcessFunction
   :reduce         CljReduceFunction
   :window         CljWindowFunction
   :sink           CljSinkFunction
   :source         CljSourceFunction
   :key-selector   CljKeySelector
   :co-flat-map    CljCoFlatMapFunction
   :process-window CljProcessWindowFunction
   :simple-reduce  CljSimpleReduceFunction})

(defn flink-fn [args]
  (let [fn-class (keyword->fn-class (:fn args))
        ctor (.getConstructor fn-class (into-array Class [APersistentMap]))
        args     (ensure-namespace args)]
    (.newInstance ctor (into-array [args]))))

(defn type-info-of [class-or-obj]
  (let [cls (if (class? class-or-obj)
              class-or-obj
              (type class-or-obj))]
    (TypeInformation/of ^Class cls)))

(defmacro fdef [name & {:as body}]
  (let [body (ensure-namespace body)]
    `(def ~name (flink-fn ~body))))

(defn timestamp-assigner [& {:as body}]
  (let [body (ensure-namespace body)]
    (CljTimestampAssigner. body)))
