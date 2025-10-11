package bufferpool;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * bufferpool.HeapFile should know nothing about page. Now using NIO with FileChannel to use
 * ByteBuffer.
 */
public class HeapFile {

  private static final String FILE_PATH = "./data/heap";

  // Keep FileChannel open while the system is running
  private final FileChannel fileChannel;

  private static final Logger logger = LogManager.getLogger(HeapFile.class);

  public HeapFile() {
    try {
      this.fileChannel =
          FileChannel.open(
              Path.of(FILE_PATH),
              StandardOpenOption.READ,
              StandardOpenOption.WRITE,
              StandardOpenOption.CREATE);
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Heap file was not found or could not be opened under %s", FILE_PATH), e);
    }
  }

  FileChannel getFileChannel() {
    return this.fileChannel;
  }

  public void close() throws IOException {
    fileChannel.close();
  }
}
