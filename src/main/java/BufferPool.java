import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class BufferPool {

    private static byte[] buffer;
    private static final int PAGE_SIZE_BYTES = 4096;
    private static final int STORAGE_BLOCK_SIZE = 4096;

    // let's say page directory is not saved inside buffer pool for now.
    private static final Map<Integer, PageDirectoryEntry> pageDirectory = new HashMap<>();
    private static final PageTable pageTable = new PageTable();
    private static final HeapFile heapFile = new HeapFile();

    BufferPool(int bufferPoolSizeBytes){
        BufferPool.buffer = new byte[bufferPoolSizeBytes];
        // touch each memory so that physical memory are allocated.
        for (int i = 0; i < BufferPool.buffer.length; i += BufferPool.STORAGE_BLOCK_SIZE) {
            BufferPool.buffer[i] = 0;
        }

        // TODO: load and save pageDirectory from disk.
    }

    public ByteBuffer getFrame(int pageID){
        if(!BufferPool.pageTable.isPageLoaded(pageID)){
            try {
                BufferPool.loadPage(pageID);
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format("Failed to load page %d from heap file", pageID), e
                );
            }
        }
        int offset = pageTable.getFrameOffset(pageID);
        // return pointer without memory copy.
        return ByteBuffer.wrap(buffer, offset, BufferPool.PAGE_SIZE_BYTES);
    }

    private static void loadPage(int pageID) throws IOException {
        int bufferTargetOffset = BufferPool.pageTable.getFreeFrameID()*BufferPool.PAGE_SIZE_BYTES;
        if (!BufferPool.pageDirectory.containsKey(pageID)){
            throw new IllegalStateException(String.format("pageID %d not found on heap file.", pageID));
        }
        BufferPool.heapFile.copyOnBuffer(BufferPool.pageDirectory.get(pageID).offset, BufferPool.PAGE_SIZE_BYTES,bufferTargetOffset, BufferPool.buffer);
        BufferPool.pageTable.afterPageLoad(pageID, bufferTargetOffset);
    }

}
