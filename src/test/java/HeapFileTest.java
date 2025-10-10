import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Random;

public class HeapFileTest {

    @Test
    void testWRToOffsetZero() throws IOException {
        // create random bytes
        int page_size = 4096;
        byte[] bytes = new byte[page_size];
        Random random = new Random();
        random.nextBytes(bytes);

        // write fo frame
        ByteBuffer buffer = ByteBuffer.allocate(page_size);
        Frame writeFrame = new Frame(buffer, 0);
        writeFrame.write(bytes);


        // write
        HeapFile file = new HeapFile();
        file.writePage(0, writeFrame);

        // read
        ByteBuffer readBuffer = ByteBuffer.allocate(page_size);
        Frame readFrame = new Frame(readBuffer, 1);
        file.readPage(0, readFrame);

        // compare
        for (int i = 0; i < writeFrame.getBuffer().limit(); i++) {
            assertEquals(writeFrame.getBuffer().get(i), readFrame.getBuffer().get(i));
        }

    }
}
