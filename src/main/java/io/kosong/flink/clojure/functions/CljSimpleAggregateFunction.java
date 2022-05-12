package io.kosong.flink.clojure.functions;

import clojure.java.api.Clojure;
import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Namespace;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class CljSimpleAggregateFunction<IN, ACC, OUT> implements AggregateFunction<IN, ACC, OUT>, ResultTypeQueryable<OUT> {

    private final Namespace namespace;
    private final IFn initFn;
    private final IFn createAccumulatorFn;
    private final IFn addFn;
    private final IFn getResultFn;
    private final IFn mergeFn;
    private final TypeInformation<OUT> returnType;

    private transient Object state;
    private transient boolean initialized;

    public CljSimpleAggregateFunction(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
        createAccumulatorFn = (IFn) Keyword.intern("createAccumulator").invoke(args);
        addFn = (IFn) Keyword.intern("add").invoke(args);
        getResultFn = (IFn) Keyword.intern("getResult").invoke(args);
        mergeFn = (IFn) Keyword.intern("merge").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(namespace.getName());
        if (initFn != null) {
            state = initFn.invoke(this);
        }
        initialized = true;
    }

    public Object state() {
        return state;
    }

    @Override
    public ACC createAccumulator() {
        if (!initialized) {
            init();
        }
        return (ACC) createAccumulatorFn.invoke(this);
    }

    @Override
    public ACC add(IN value, ACC accumulator) {
        if (!initialized) {
            init();
        }
        return (ACC) addFn.invoke(this, value, accumulator);
    }

    @Override
    public OUT getResult(ACC accumulator) {
        if (!initialized) {
            init();
        }
        return (OUT) getResultFn.invoke(this, accumulator);
    }

    @Override
    public ACC merge(ACC a, ACC b) {
        if (!initialized) {
            init();
        }
        return (ACC) mergeFn.invoke(this, a, b);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return returnType;
    }
}
