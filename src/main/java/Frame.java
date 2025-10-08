import java.nio.ByteBuffer;

public class Frame {

    private final ByteBuffer buffer;
    private boolean isDirty;
    private static final int FRAME_SIZE_BYTES = 4096;

    Frame(ByteBuffer buffer_pool_slice){
        this.buffer = buffer_pool_slice;
        this.buffer.position(0).limit(FRAME_SIZE_BYTES);
    }

    void clear(){
        isDirty = false;
        this.buffer.clear();
    }

    ByteBuffer getBuffer(){
        this.buffer.position(0).limit(FRAME_SIZE_BYTES);
        return this.buffer;
    }

    void write(byte[] bytes){
        isDirty = true;
        buffer.put(bytes);
    }

}
