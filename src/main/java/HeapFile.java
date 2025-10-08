import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * HeapFile should know nothing about page.
 * Now using NIO with FileChannel to achieve block access.
 */
public class HeapFile {

    private static final String FILE_PATH = "./data/heap";
    private static final int PAGE_SIZE_BYTES = 4096;

    // Keep FileChannel open while the system is running
    private final FileChannel fileChannel;

    private static final Logger logger = LogManager.getLogger(HeapFile.class);



    public HeapFile() {
        try {
            this.fileChannel = FileChannel.open(
                    Path.of(FILE_PATH),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format("Heap file was not found or could not be opened under %s", FILE_PATH), e
            );
        }
    }

    /**
     * Read from file into a slice of the provided frame
     */
    public void readPage(int readOffset, ByteBuffer buffer) throws IOException {

        int bytesRead = 0;
        while (bytesRead < PAGE_SIZE_BYTES) {
            bytesRead += fileChannel.read(buffer, readOffset + bytesRead);
        }
    }

    /**
     * Write a slice of the provided frame into file at disk offset
     */
    public void writePage(int writeDiskOffset, ByteBuffer frame) throws IOException {
        logger.debug("writing frame to file offset " + writeDiskOffset);
        int bytesWritten = 0;
        while (bytesWritten < PAGE_SIZE_BYTES) {
            bytesWritten += fileChannel.write(frame, writeDiskOffset + bytesWritten);
        }
    }

    public void close() throws IOException {
        fileChannel.close();
    }
}