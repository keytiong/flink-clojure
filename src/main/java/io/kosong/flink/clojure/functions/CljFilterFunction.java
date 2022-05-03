package io.kosong.flink.clojure.functions;

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;


public class CljFilterFunction<T> extends RichFilterFunction<T>
        implements ResultTypeQueryable<T>, CheckpointedFunction {

    private final IFn initFn;
    private final IFn openFn;
    private final IFn closeFn;
    private final IFn initializeStateFn;
    private final IFn filterFn;
    private final IFn snapshotStateFn;
    private final TypeInformation<T> returnType;

    private transient Object state;
    private transient boolean initialized;

    public CljFilterFunction(APersistentMap args) {
        initFn = (IFn) Keyword.intern("init").invoke(args);
        openFn = (IFn) Keyword.intern("open").invoke(args);
        closeFn = (IFn) Keyword.intern("close").invoke(args);
        filterFn = (IFn) Keyword.intern("filter").invoke(args);
        initializeStateFn = (IFn) Keyword.intern("initializeState").invoke(args);
        snapshotStateFn = (IFn) Keyword.intern("snapshotState").invoke(args);
        returnType = (TypeInformation<T>) Keyword.intern("returns").invoke(args);
    }

    @Override
    public boolean filter(T value) throws Exception {
        return (Boolean) filterFn.invoke(this, value);
    }

    private void init() {
        if (initFn != null) {
            state = initFn.invoke(this);
        }
        initialized = true;
    }

    public Object state() {
        return this.state;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (!initialized) {
            init();
        }
        if (openFn != null) {
            openFn.invoke(this, parameters);
        }
    }

    @Override
    public void close() throws Exception {
        if (closeFn != null) {
            closeFn.invoke(this);
        }
        super.close();
    }

    @Override
    public TypeInformation<T> getProducedType() {
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
