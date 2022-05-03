package io.kosong.flink.clojure.functions;


import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CljSourceFunction<OUT> extends RichSourceFunction<OUT>
        implements ResultTypeQueryable<OUT>, CheckpointedFunction {

    private static final Logger log = LogManager.getLogger(CljSourceFunction.class);

    private TypeInformation<OUT> returnType;

    private transient boolean initialized = false;
    private transient Object state;

    private final IFn initFn;
    private final IFn openFn;
    private final IFn closeFn;
    private final IFn runFn ;
    private final IFn cancelFn;
    private final IFn initializeStateFn;
    private final IFn snapshotStateFn;

    public CljSourceFunction(APersistentMap args) {
        initFn = (IFn) Keyword.intern("init").invoke(args);
        openFn = (IFn) Keyword.intern("open").invoke(args);
        closeFn = (IFn) Keyword.intern("close").invoke(args);
        runFn = (IFn) Keyword.intern("run").invoke(args);
        cancelFn = (IFn) Keyword.intern("cancel").invoke(args);
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

    public Object state() {
        return this.state;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        if (runFn == null) {
            throw new IllegalStateException("run function is null");
        }
        runFn.invoke(this, ctx);
    }

    @Override
    public void cancel() {
        if (cancelFn != null) {
            cancelFn.invoke(this);
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
