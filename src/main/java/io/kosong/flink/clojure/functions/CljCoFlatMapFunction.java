package io.kosong.flink.clojure.functions;

import clojure.java.api.Clojure;
import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Namespace;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class CljCoFlatMapFunction<IN1, IN2, OUT> extends RichCoFlatMapFunction<IN1, IN2, OUT> implements ResultTypeQueryable<OUT>, CheckpointedFunction {

    private final Namespace namespace;
    private final IFn initFn;
    private final IFn openFn;
    private final IFn closeFn;
    private final IFn initializeStateFn;
    private final IFn flatMap1Fn;
    private final IFn flatMap2Fn;
    private final IFn snapshotStateFn;
    private final TypeInformation<OUT> returnType;

    private transient Object state;
    private transient boolean initialized;

    public CljCoFlatMapFunction(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
        openFn = (IFn) Keyword.intern("open").invoke(args);
        closeFn = (IFn) Keyword.intern("close").invoke(args);
        flatMap1Fn = (IFn) Keyword.intern("flatMap1").invoke(args);
        flatMap2Fn = (IFn) Keyword.intern("flatMap2").invoke(args);
        initializeStateFn = (IFn) Keyword.intern("initializeState").invoke(args);
        snapshotStateFn = (IFn) Keyword.intern("snapshotState").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
    }

    @Override
    public void flatMap1(IN1 value, Collector<OUT> out) throws Exception {
        flatMap1Fn.invoke(this, value, out);
    }

    @Override
    public void flatMap2(IN2 value, Collector<OUT> out) throws Exception {
        flatMap2Fn.invoke(this, value, out);
    }

    private void init() {

        Clojure.var("clojure.core/require").invoke(namespace.getName());

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
}
