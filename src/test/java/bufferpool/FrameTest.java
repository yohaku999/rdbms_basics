package bufferpool;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class FrameTest {

  @Test
  void testWRToOffsetZero() throws IOException {
    // create random bytes
    int page_size = BufferPool.PAGE_SIZE_BYTES;
    byte[] bytes = new byte[page_size];
    Random random = new Random();
    random.nextBytes(bytes);

    // write fo frame
    ByteBuffer buffer = ByteBuffer.allocate(page_size);
    Frame writeFrame = new Frame(buffer, 0);
    writeFrame.append_bytes(bytes);
    HeapFile heapFile = new HeapFile();
    writeFrame.writeToDisk(heapFile, 0);

    // read
    ByteBuffer readBuffer = ByteBuffer.allocate(page_size);
    Frame readFrame = new Frame(readBuffer, 1);
    readFrame.loadFrom(heapFile, 0);

    // compare
    for (int i = 0; i < writeFrame.read().limit(); i++) {
      assertEquals(writeFrame.read().get(i), readFrame.read().get(i));
    }
  }
}
