package io.kosong.flink.clojure.functions;


import clojure.java.api.Clojure;
import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Namespace;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CljSinkFunction<IN> extends RichSinkFunction<IN>
implements CheckpointedFunction {

    private static final Logger log = LogManager.getLogger(CljSinkFunction.class);

    private transient boolean initialized = false;
    private transient Object state;

    private final Namespace namespace;
    private final IFn initFn;
    private final IFn openFn;
    private final IFn closeFn;
    private final IFn invokeFn;
    private final IFn initializeStateFn;
    private final IFn snapshotStateFn;
    private final IFn writeWatermarkFn;
    private final IFn finishFn;


    public CljSinkFunction(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
        invokeFn = (IFn) Keyword.intern("invoke").invoke(args);
        openFn = (IFn) Keyword.intern("open").invoke(args);
        closeFn = (IFn) Keyword.intern("close").invoke(args);
        initializeStateFn = (IFn) Keyword.intern("initializeState").invoke(args);
        snapshotStateFn = (IFn) Keyword.intern("snapshotState").invoke(args);
        writeWatermarkFn = (IFn) Keyword.intern("writeWatermark").invoke(args);
        finishFn = (IFn) Keyword.intern("finish").invoke(args);
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(namespace.getName());
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

    @Override
    public void invoke(IN value, Context context) throws Exception {
        invokeFn.invoke(this, value, context);
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        super.writeWatermark(watermark);
        if (writeWatermarkFn != null) {
            writeWatermarkFn.invoke(this, watermark);
        }
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        if (finishFn != null) {
            finishFn.invoke(this);
        }
    }

    public Object state() {
        return this.state;
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
