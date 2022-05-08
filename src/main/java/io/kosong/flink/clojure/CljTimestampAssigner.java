package io.kosong.flink.clojure;

import clojure.java.api.Clojure;
import clojure.lang.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.time.Instant;

public class CljTimestampAssigner<T> implements SerializableTimestampAssigner<T> {

    private final AFunction extractTimestampFn;
    private final Namespace namespace;

    private transient boolean initialized;

    public CljTimestampAssigner(APersistentMap args) {
        namespace = (Namespace) Keyword.intern("ns").invoke(args);
        extractTimestampFn = (AFunction) Keyword.intern("extractTimestamp").invoke(args);
    }

    private void init() {
        Clojure.var("clojure.core/require").invoke(namespace.getName());
        initialized = true;
    }

    @Override
    public long extractTimestamp(T element, long recordTimestamp) {
        if (!initialized) {
            init();
        }
        /*
        Instant eventTime = (Instant) Keyword.intern("start-time").invoke(element);
        return eventTime.toEpochMilli();
         */

        return (long) extractTimestampFn.invoke(element, recordTimestamp);
    }
}
