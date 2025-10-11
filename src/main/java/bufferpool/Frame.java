package bufferpool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Frame {

  private final ByteBuffer buffer;

  private final int id;
  private boolean isDirty;
  private final AtomicBoolean isPinned = new AtomicBoolean(false);
  private static final Logger logger = LogManager.getLogger(Frame.class);

  Frame(ByteBuffer buffer_pool_slice, int id) {
    this.buffer = buffer_pool_slice;
    this.buffer.position(0).limit(BufferPool.PAGE_SIZE_BYTES);
    this.id = id;
  }

  boolean pin() {
    boolean result = isPinned.compareAndSet(false, true);
    if (result) {
      logger.debug("pinned frameID " + this.id);
    }
    return result;
  }

  void unpin() {
    isPinned.set(false);
    logger.debug("unpinned frameID " + this.id);
  }

  boolean isDirty() {
    return isDirty;
  }

  void clear() {
    isDirty = false;
    this.buffer.clear();
  }

  /**
   * load a page from heapFile based starting from readOffset.
   *
   * @param heapFile
   * @param readOffset
   * @throws IOException
   */
  public void loadFrom(HeapFile heapFile, int readOffset) throws IOException {
    int bytesRead = 0;
    while (bytesRead < BufferPool.PAGE_SIZE_BYTES) {
      int n = heapFile.getFileChannel().read(this.buffer, readOffset + bytesRead);
      if (n < 0) break;
      bytesRead += n;
    }
    buffer.flip();
  }

  /**
   * write the buffer of this frame to disk.
   *
   * @param heapFile
   * @param writeOffset
   * @throws IOException
   */
  public void writeToDisk(HeapFile heapFile, int writeOffset) throws IOException {
    logger.debug("writing frame " + this.id + " to file offset " + writeOffset);
    buffer.flip();
    int bytesWritten = 0;
    while (bytesWritten < BufferPool.PAGE_SIZE_BYTES) {
      bytesWritten += heapFile.getFileChannel().write(this.buffer, writeOffset + bytesWritten);
      System.out.println(bytesWritten);
    }
    this.isDirty = false;
  }

  void append_bytes(byte[] bytes) {
    isDirty = true;
    buffer.put(bytes);
  }

  int getID() {
    return this.id;
  }

  boolean isPinned() {
    return isPinned.get();
  }

  ByteBuffer read() {
    return this.buffer;
  }
}
