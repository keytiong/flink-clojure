package io.kosong.flink.clojure.functions;

import clojure.java.api.Clojure;
import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Namespace;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

public class CljSimpleReduceFunction<T> implements ReduceFunction<T> {

    private final Namespace namespace;
    private final IFn initFn;
    private final IFn reduceFn;
    private final TypeInformation<T> returnType;

    private transient Object state;
    private transient boolean initialized;

    public CljSimpleReduceFunction(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
        reduceFn = (IFn) Keyword.intern("reduce").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(namespace.getName());
        if (initFn != null) {
            state = initFn.invoke(this);
        }
        initialized = true;
    }

    @Override
    public T reduce(T value1, T value2) throws Exception {
        if (!initialized) {
            init();
        }
        return (T) reduceFn.invoke(this, value1, value2);
    }

    public TypeInformation getProducedType() {
        return returnType;
    }

}
