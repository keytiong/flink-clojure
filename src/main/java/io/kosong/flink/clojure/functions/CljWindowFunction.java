package io.kosong.flink.clojure.functions;

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class CljWindowFunction<IN, OUT, KEY, W extends Window> extends RichWindowFunction<IN, OUT, KEY, W> implements ResultTypeQueryable<OUT>, CheckpointedFunction {

    private final IFn initFn;
    private final IFn initializeStateFn;
    private final IFn applyFn;
    private final IFn snapshotStateFn;
    private final TypeInformation<OUT> returnType;

    private transient Object state;
    private transient boolean initialized;

    public CljWindowFunction(APersistentMap args) {
        initFn = (IFn) Keyword.intern("init").invoke(args);
        applyFn = (IFn) Keyword.intern("apply").invoke(args);
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
    public void apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception {
        applyFn.invoke(this, key, window, input, out);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return returnType;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (snapshotStateFn != null) {
            snapshotStateFn.invoke(this, context);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (!initialized) {
            init();
        }
        if (initializeStateFn != null) {
            initializeStateFn.invoke(this, context);
        }
    }
}