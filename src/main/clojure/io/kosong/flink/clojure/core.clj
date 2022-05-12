(ns io.kosong.flink.clojure.core
  (:import (io.kosong.flink.clojure NippySerializer CljTimestampAssigner)
           (clojure.lang PersistentVector PersistentHashMap PersistentHashSet PersistentArrayMap PersistentStructMap
                         PersistentTreeMap PersistentTreeSet PersistentList APersistentMap)
           (io.kosong.flink.clojure.functions CljMapFunction CljFlatMapFunction CljKeyedProcessFunction
                                              CljProcessFunction CljSinkFunction CljSourceFunction CljKeySelector
                                              CljReduceFunction CljWindowFunction CljFilterFunction CljCoFlatMapFunction CljProcessWindowFunction CljSimpleReduceFunction CljSimpleAggregateFunction CljBroadcastProcessFunction CljCoProcessFunction CljKeyedBroadcastProcessFunction CljProcessJoinFunction CljKeyedCoProcessFunction CljCoMapFunction CljAsyncFunction CljProcessAllWindowFunction CljParallelSourceFunction)
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
  {:source                  CljSourceFunction
   :parallel-source         CljParallelSourceFunction

   :map                     CljMapFunction
   :flat-map                CljFlatMapFunction
   :filter                  CljFilterFunction
   :reduce                  CljReduceFunction
   :process                 CljProcessFunction

   :key-selector            CljKeySelector
   :keyed-process           CljKeyedProcessFunction
   :keyed-broadcast-process CljKeyedBroadcastProcessFunction
   :keyed-co-process        CljKeyedCoProcessFunction

   :window                  CljWindowFunction
   :process-window          CljProcessWindowFunction
   :process-all-window      CljProcessAllWindowFunction
   :simple-reduce           CljSimpleReduceFunction
   :simple-aggregate        CljSimpleAggregateFunction

   :co-map                  CljCoMapFunction
   :co-flat-map             CljCoFlatMapFunction
   :co-process              CljCoProcessFunction
   :broadcast-process       CljBroadcastProcessFunction
   :process-join            CljProcessJoinFunction

   :async                   CljAsyncFunction

   :sink                    CljSinkFunction})

(defn flink-fn [& {:as args}]
  (let [fn-class (keyword->fn-class (:fn args))
        ctor     (.getConstructor fn-class (into-array Class [APersistentMap]))
        args     (ensure-namespace args)]
    (.newInstance ctor (into-array [args]))))

(defn type-info-of [class-or-obj]
  (let [cls (if (class? class-or-obj)
              class-or-obj
              (type class-or-obj))]
    (TypeInformation/of ^Class cls)))

(defmacro fdef [name & {:as body}]
  `(def ~name (flink-fn ~body)))

(defn timestamp-assigner [& {:as body}]
  (let [body (ensure-namespace body)]
    (CljTimestampAssigner. body)))
