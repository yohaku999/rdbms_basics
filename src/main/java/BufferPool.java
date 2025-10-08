import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

/**
 * ByteBuffer is used to accomplish direct_IO and clener code without extra memory copy.
 * Callers of BufferPool should know anything about Frame. Transparent Access to pages on the healfile and othres must be provided.
 */
public class BufferPool {

    private final List<Frame> frames = new ArrayList<>();
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
            frames.add(new Frame(frameView.slice()));
        }

        pageTable = new PageTable(frameNum);

        // TODO: load and save pageDirectory from disk.
    }

    public Frame getPage(int pageID){
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

    public Frame getNewPage() throws IOException {
        int pageID = this.pageDirectory.issueNewPage();
        // since new page is required only for write, we can always return empty frame.
        OptionalInt frameID = this.pageTable.getFreeFrameID(pageID);
        if (frameID.isEmpty()){
            // TODO: have to deal with concurrency problem
            this.evictPage();
            frameID = this.pageTable.getFreeFrameID(pageID);
        }
        Frame frame = this.frames.get(frameID.getAsInt());
        frame.clear();
        return frame;
    }

    private void loadPageOnBuffer(int pageID) throws IOException {
        OptionalInt frameID = this.pageTable.getFreeFrameID(pageID);
        if (frameID.isEmpty()){
            // TODO: have to deal with concurrency problem.まあでもどっちが先でも一旦問題ないのか。
            this.evictPage();
            frameID = this.pageTable.getFreeFrameID(pageID);
        }
        this.frames.get(frameID.getAsInt()).clear();
        this.heapFile.readPage(this.pageDirectory.getOffset(pageID), this.frames.get(frameID.getAsInt()));
    }

    private void writeFrameToStorage(int pageID) throws IOException {
        logger.debug("writing page " + pageID + " storage");
        int pageOffset = this.pageDirectory.getOffset(pageID);
        int frameID = this.pageTable.getFrameID(pageID);
        this.heapFile.writePage(pageOffset, this.frames.get(frameID));
        this.pageTable.markPageClean(pageID);
    }

    private void evictPage() throws IOException {
        // TODO: implement eviction policy, which should be DIed in the end.
        // decide whitch page to evict.
        int pageID = 0;
        logger.debug("evicting page " + pageID);
        // TODO: write corresponding WAL data on disk.
        // check if it is dirty
        if(this.pageTable.isFrameDirty(pageID)){
            this.writeFrameToStorage(pageID);
            this.pageTable.markPageClean(pageID);
        }
        // write page to evict on disk
        this.pageTable.openFrame(pageID);
    }

}
