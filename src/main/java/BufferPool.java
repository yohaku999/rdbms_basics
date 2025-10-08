import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * ByteBuffer is used to accomplish direct_IO and clener code without extra memory copy.
 * Callers of BufferPool should know anything about Frame. Transparent Access to pages on the healfile must be provided.
 */
public class BufferPool {

    private final List<ByteBuffer> frames = new ArrayList<>();;
    private static final int PAGE_SIZE_BYTES = 4096;
    private static final int STORAGE_BLOCK_SIZE = 4096;

    // let's say page directory is not saved inside buffer pool for now.
    private final PageDirectory pageDirectory = new PageDirectory();
    private PageTable pageTable;
    private HeapFile heapFile;

    private static final Logger logger = LogManager.getLogger(BufferPool.class);
    BufferPool(int bufferPoolSizeBytes, HeapFile heapFile){
        this.heapFile = heapFile;
        ByteBuffer buffer = ByteBuffer.allocate(bufferPoolSizeBytes);

        // Touch each block to ensure physical memory allocation
        for (int i = 0; i < buffer.capacity(); i += STORAGE_BLOCK_SIZE) {
            buffer.put(i, (byte) 0);
        }

        // allocate buffer to ecah Frame.
        int frameNum = bufferPoolSizeBytes / PAGE_SIZE_BYTES;

        for (int i = 0; i < frameNum; i++) {
            ByteBuffer frameView = buffer.duplicate();
            frameView.position(i * PAGE_SIZE_BYTES);
            frameView.limit((i + 1) * PAGE_SIZE_BYTES);
            ByteBuffer frameSlice = frameView.slice();
            frames.add(frameSlice);
        }

        pageTable = new PageTable(frameNum);

        // TODO: load and save pageDirectory from disk.
    }

    public ByteBuffer getPage(int pageID){
        logger.debug("get page " + pageID);
        if(!this.pageTable.isPageLoaded(pageID)){
            logger.debug("was not loaded " + pageID);
            try {
                this.loadPageOnBuffer(pageID);
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format("Failed to load page %d from heap file", pageID), e
                );
            }
        }
        int frameID = pageTable.getFrameID(pageID);
        return frames.get(frameID);
    }

    public ByteBuffer getNewPage(){
        int pageID = this.pageDirectory.issueNewPage();
        // since new page is required only for write, we can always return empty frame.
        int frameID = this.pageTable.getFreeFrameID(pageID);
        ByteBuffer frame = this.frames.get(frameID);
        frame.clear();
        return frame;
    }

    private void loadPageOnBuffer(int pageID) throws IOException {
        int frameID = this.pageTable.getFreeFrameID(pageID);
        this.frames.get(frameID).clear();
        this.heapFile.readPage(this.pageDirectory.getOffset(pageID), this.frames.get(frameID));
    }

    private void writePageToStorage(int pageID) throws IOException {
        int pageOffset = this.pageDirectory.getOffset(pageID);
        int frameID = this.pageTable.getFrameID(pageID);
        this.heapFile.writePage(pageOffset, this.frames.get(frameID));
        this.pageTable.markPageClean(pageID);
    }

    private void evictPage() throws IOException {
        // TODO: implement eviction policy
        // decide whitch page to evict.
        int pageID = 0;
        // TODO: write corresponding WAL data on disk.
        // check if it is dirty
        if(this.pageTable.isPageDirty(pageID)){
            this.writePageToStorage(pageID);
            this.pageTable.markPageClean(pageID);
        }
        // write page to evict on disk
        this.pageTable.openFrame(pageID);
    }

}
