package io.kosong.flink.clojure;


import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NippySerializer extends Serializer<Object> {

    private static final IFn freeze;
    private static final IFn thaw;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("taoensso.nippy"));
        freeze = Clojure.var("taoensso.nippy/freeze");
        thaw = Clojure.var("taoensso.nippy/thaw");
    }

    public NippySerializer() {
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        try {
            byte[] barr = (byte[]) freeze.invoke(object);
            output.writeInt(barr.length, true);
            output.writeBytes(barr);
            output.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class<Object> type) {
        try {
            int bsize = input.readInt(true);
            byte[] barr = new byte[bsize];
            input.readBytes(barr);
            return thaw.invoke(barr);
        } catch (Exception e) {
            throw new RuntimeException("Could not create " + type, e);
        }
    }
}
