package io.kosong.flink.clojure.functions;


import clojure.java.api.Clojure;
import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Namespace;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CljKeyedProcessFunction<K, I, O> extends KeyedProcessFunction<K, I, O>
        implements ResultTypeQueryable<O>, CheckpointedFunction {

    private final TypeInformation<O> returnType;

    private transient Object state;
    private transient boolean initialized;

    private final IFn openFn;
    private final IFn closeFn;
    private final IFn onTimerFn;
    private final IFn processElementFn;
    private final Namespace namespace;
    private final IFn initFn;
    private final IFn initializeStateFn;
    private final IFn snapshotStateFn;

    public CljKeyedProcessFunction(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
        openFn = (IFn) Keyword.intern("open").invoke(args);
        closeFn = (IFn) Keyword.intern("close").invoke(args);
        processElementFn = (IFn) Keyword.intern("processElement").invoke(args);
        onTimerFn = (IFn) Keyword.intern("onTimer").invoke(args);
        initializeStateFn = (IFn) Keyword.intern("initializeState").invoke(args);
        snapshotStateFn = (IFn) Keyword.intern("snapshotState").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
    }

    public Object state() {
        return this.state;
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(namespace.getName());
        if (initFn != null) {
            this.state = initFn.invoke(this);
        }
        initialized = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

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
    }

    @Override
    public void processElement(I value, Context ctx, Collector<O> out) throws Exception {
        processElementFn.invoke(this, value, ctx, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {
        onTimerFn.invoke(this, timestamp, ctx, out);
    }

    @Override
    public TypeInformation<O> getProducedType() {
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
