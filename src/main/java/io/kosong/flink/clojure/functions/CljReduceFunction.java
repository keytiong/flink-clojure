package io.kosong.flink.clojure.functions;

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class CljReduceFunction<T> extends RichReduceFunction<T> implements ResultTypeQueryable<T>, CheckpointedFunction {

    private final IFn initFn;
    private final IFn initializeStateFn;
    private final IFn reduceFn;
    private final IFn snapshotStateFn;
    private final TypeInformation<T> returnType;

    private transient Object state;
    private transient boolean initialized;

    public CljReduceFunction(APersistentMap args) {
        initFn = (IFn) Keyword.intern("init").invoke(args);
        reduceFn = (IFn) Keyword.intern("reduce").invoke(args);
        initializeStateFn = (IFn) Keyword.intern("initializeState").invoke(args);
        snapshotStateFn = (IFn) Keyword.intern("snapshotState").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
    }

    private void init() {
        if (initFn != null) {
            state = initFn.invoke(this);
        }
        initialized = true;
    }

    @Override
    public T reduce(T value1, T value2) throws Exception {
        return (T) reduceFn.invoke(this, value1, value2);
    }

    public TypeInformation getProducedType() {
        return returnType;
    }


    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (snapshotStateFn != null) {
            snapshotStateFn.invoke(this, context);
        }
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (!initialized) {
            init();
        }
        if (initializeStateFn != null) {
            initializeStateFn.invoke(this, context);
        }
    }
}