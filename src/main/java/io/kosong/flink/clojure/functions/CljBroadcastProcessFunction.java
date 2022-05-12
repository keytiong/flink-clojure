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
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class CljBroadcastProcessFunction<IN1, IN2, OUT> extends BroadcastProcessFunction<IN1, IN2, OUT>
        implements ResultTypeQueryable<OUT>, CheckpointedFunction {

    private transient Object state;
    private transient boolean initialized;

    private final Namespace namespace;
    private final TypeInformation<OUT> returnType;
    private final IFn initFn;
    private final IFn openFn;
    private final IFn closeFn;
    private final IFn snapshotStateFn;
    private final IFn initializeStateFn;
    private final IFn processElementFn;
    private final IFn processBroadcastElementFn;

    public CljBroadcastProcessFunction(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
        openFn = (IFn) Keyword.intern("open").invoke(args);
        closeFn = (IFn) Keyword.intern("close").invoke(args);
        initializeStateFn = (IFn) Keyword.intern("initializeState").invoke(args);
        snapshotStateFn = (IFn) Keyword.intern("snapshotState").invoke(args);
        processElementFn = (IFn) Keyword.intern("processElement").invoke(args);
        processBroadcastElementFn = (IFn) Keyword.intern("processBroadcastElement").invoke(args);
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(namespace.getName());
        if (initFn != null) {
            this.state = initFn.invoke(this);
        }
        initialized = true;
    }

    public Object state() {
        return this.state;
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
    public void processElement(IN1 value, BroadcastProcessFunction<IN1, IN2, OUT>.ReadOnlyContext ctx, Collector<OUT> out) throws Exception {
        processElementFn.invoke(this, value, ctx, out);
    }

    @Override
    public void processBroadcastElement(IN2 value, BroadcastProcessFunction<IN1, IN2, OUT>.Context ctx, Collector<OUT> out) throws Exception {
        processBroadcastElementFn.invoke(this, value, ctx, out);
    }

}
