import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class HeapFile {

    private static final String FILE_PATH = "./data/heap";

    // Keep random access file opened while the system is running, since we expect to have single heap file like SQLite.
    // disk scan only required block by using RandomAccessFile.
    private static RandomAccessFile file;

    HeapFile(){
        try {
            file = new RandomAccessFile(HeapFile.FILE_PATH, "r");
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(String.format("heap file was not found under %s", HeapFile.FILE_PATH));
        }
    }

    void copyOnBuffer(int read_offset, int copy_byte_size, int write_offset, byte[] buffer) throws IOException {
        HeapFile.file.seek(read_offset);
        HeapFile.file.readFully(buffer, write_offset, copy_byte_size);
    }
}
