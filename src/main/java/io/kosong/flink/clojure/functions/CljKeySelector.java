package io.kosong.flink.clojure.functions;

import clojure.java.api.Clojure;
import clojure.lang.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class CljKeySelector<IN, KEY> implements KeySelector<IN, KEY>, ResultTypeQueryable<KEY> {

    private final TypeInformation<KEY> returnType;
    private final IFn getKeyFn;
    private final Namespace namespace;
    private final IFn initFn;

    private transient Object state;
    private transient boolean initialized;

    public CljKeySelector(APersistentMap args) {
        getKeyFn = (IFn) Keyword.intern("getKey").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        initFn = (IFn) Keyword.intern("init").invoke(args);
    }

    public Object state() {
        return state;
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(namespace.getName());
        if (initFn != null) {
            this.state = initFn.invoke(this);
        }
        initialized = true;
    }

    @Override
    public KEY getKey(IN value) throws Exception {
        if (!initialized) {
            init();
        }
        return (KEY) getKeyFn.invoke(this, value);
    }

    @Override
    public TypeInformation<KEY> getProducedType() {
        return returnType;
    }
}
