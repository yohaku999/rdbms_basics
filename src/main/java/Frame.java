import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class Frame {

    private final ByteBuffer buffer;

    private final int id;
    private boolean isDirty;
    private final AtomicBoolean isPinned = new AtomicBoolean(false);
    private static final int FRAME_SIZE_BYTES = 4096;

    private static final Logger logger = LogManager.getLogger(Frame.class);
    Frame(ByteBuffer buffer_pool_slice, int id){
        this.buffer = buffer_pool_slice;
        this.buffer.position(0).limit(FRAME_SIZE_BYTES);
        this.id = id;
    }

    int getID(){
        return this.id;
    }

    boolean isPinned(){
        return isPinned.get();
    }


    boolean pin(){
        boolean result =  isPinned.compareAndSet(false, true);
        if (result){
            logger.debug("pinned frameID " + this.id);
        }
        return result;
    }

    void unpin(){
        isPinned.set(false);
        logger.debug("unpinned frameID " + this.id);
    }

    boolean isDirty() {
        return isDirty;
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
        logger.debug("write to frame " + id);
        isDirty = true;
        buffer.put(bytes);
    }

}
