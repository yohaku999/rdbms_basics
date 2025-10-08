import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Random;

public class HeapFileTest {

    @Test
    void testWRToOffsetZero() throws IOException {
        // create buffer to write
        int page_size = 4096;
        byte[] bytes = new byte[page_size];
        Random random = new Random();
        random.nextBytes(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // write
        HeapFile file = new HeapFile();
        file.writePage(0, buffer);

        // read and compare
        ByteBuffer read_buffer = ByteBuffer.allocate(page_size);
        file.readPage(0, read_buffer);
        assertEquals(read_buffer,buffer);
    }
}
