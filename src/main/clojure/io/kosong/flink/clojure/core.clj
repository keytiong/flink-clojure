(ns io.kosong.flink.clojure.core
  (:import (io.kosong.flink.clojure NippySerializer)
           (clojure.lang PersistentVector PersistentHashMap PersistentHashSet PersistentArrayMap PersistentStructMap
                         PersistentTreeMap PersistentTreeSet PersistentList)
           (io.kosong.flink.clojure.functions CljMapFunction CljFlatMapFunction CljKeyedProcessFunction
                                              CljProcessFunction CljSinkFunction CljSourceFunction CljKeySelector
                                              CljReduceFunction CljWindowFunction)
           (org.apache.flink.api.common.typeinfo TypeInformation)))

(def ^:private clojure-collection-types
  [PersistentList
   PersistentVector  PersistentHashSet PersistentTreeSet
   PersistentHashMap PersistentArrayMap PersistentStructMap PersistentTreeMap])

(defn register-clojure-types [env]
  (let [exec-config (.getConfig env)]
    (doseq [type clojure-collection-types]
      (.registerTypeWithKryoSerializer exec-config type NippySerializer))
    env))

(defmulti make-fn :fn :default nil)

(defmethod make-fn :map [& {:as args}]
  (CljMapFunction. args))

(defmethod make-fn :flat-map [& {:as args}]
  (CljFlatMapFunction. args))

(defmethod make-fn :keyed-process [& {:as args}]
  (CljKeyedProcessFunction. args))

(defmethod make-fn :process [& {:as args}]
  (CljProcessFunction. args))

(defmethod make-fn :reduce [& {:as args}]
  (CljReduceFunction. args))

(defmethod make-fn :window [& {:as args}]
  (CljWindowFunction. args))

(defmethod make-fn :sink [& {:as args}]
  (CljSinkFunction. args))

(defmethod make-fn :source [& {:as args}]
  (CljSourceFunction. args))

(defmethod make-fn :key-selector [& {:as args}]
  (CljKeySelector. args))

(defn type-info-of [class-or-obj]
  (let [cls (if (class? class-or-obj)
              class-or-obj
              (type class-or-obj))]
    (TypeInformation/of ^Class cls)))

(defmacro fdef [name & {:as body}]
  `(def ~name (make-fn ~body)))
