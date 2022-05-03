package io.kosong.flink.clojure.functions;

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class CljKeySelector<IN, KEY> implements KeySelector<IN, KEY>, ResultTypeQueryable<KEY> {

    private final TypeInformation<KEY> returnType;
    private final IFn getKeyFn;

    public CljKeySelector(APersistentMap args) {
        getKeyFn = (IFn) Keyword.intern("getKey").invoke(args);
        returnType = (TypeInformation) Keyword.intern("returns").invoke(args);
    }

    @Override
    public KEY getKey(IN value) throws Exception {
        return (KEY) getKeyFn.invoke(this, value);
    }

    @Override
    public TypeInformation<KEY> getProducedType() {
        return returnType;
    }
}
