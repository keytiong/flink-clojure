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
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class CljKeyedCoProcessFunction<K, IN1, IN2, OUT> extends KeyedCoProcessFunction<K, IN1, IN2, OUT>
        implements ResultTypeQueryable<OUT>, CheckpointedFunction {

    private final TypeInformation<OUT> returnType;

    private transient Object state;
    private transient boolean initialized;

    private final Namespace namespace;
    private final IFn initFn;
    private final IFn openFn;
    private final IFn closeFn;
    private final IFn initializeStateFn;
    private final IFn snapshotStateFn;
    private final IFn processElement1Fn;
    private final IFn processElement2Fn;
    private final IFn onTimerFn;

    public CljKeyedCoProcessFunction(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
        openFn = (IFn) Keyword.intern("open").invoke(args);
        closeFn = (IFn) Keyword.intern("close").invoke(args);
        initializeStateFn = (IFn) Keyword.intern("initializeState").invoke(args);
        snapshotStateFn = (IFn) Keyword.intern("snapshotState").invoke(args);
        processElement1Fn = (IFn) Keyword.intern("processElement1").invoke(args);
        processElement2Fn = (IFn) Keyword.intern("processElement2").invoke(args);
        onTimerFn = (IFn) Keyword.intern("onTimer").invoke(args);

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
    @Override
    public void processElement1(IN1 value, Context ctx, Collector<OUT> out) throws Exception {
        processElement1Fn.invoke(this, value, ctx, out);
    }

    @Override
    public void processElement2(IN2 value, Context ctx, Collector<OUT> out) throws Exception {
        processElement2Fn.invoke(this, value, ctx, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        if (onTimerFn != null) {
            onTimerFn.invoke(this, timestamp, timestamp, ctx, out);
        }
    }
}
