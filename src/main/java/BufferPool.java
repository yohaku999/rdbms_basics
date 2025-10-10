import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    private final Object glock = new Object();

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
            frames.add(new Frame(frameView.slice(), i));
        }

        pageTable = new PageTable(frames);

        // TODO: load and save pageDirectory from disk.
    }

    public Frame getPage(int pageID) throws IOException {
        // OPTIMIZE:this method should run concurrently if pageIDs are independent.
        Lock lock = getLockForPage(pageID);
        lock.lock();
        synchronized (glock){
            if(this.pageTable.hasEvicted(pageID)) {
                try {
                    // TODO: do buffer pool user need to know if getting freem frame or freed frame?
                    OptionalInt frameID = this.pageTable.getUnPinnedCleanFrameID(pageID);
                    if (frameID.isEmpty()) {
                        this.evictPage();
                        frameID = this.pageTable.getUnPinnedCleanFrameID(pageID);
                    }
                    // this.frames.get(frameID.getAsInt()).clear();
                    // 4kb単位なのでreadの時は上書きは不要なはず
                    this.heapFile.readPage(this.pageDirectory.getOffset(pageID),
                            this.frames.get(frameID.getAsInt()));
                } finally {
                    lock.unlock();
                }
            }
            int frameID = pageTable.getFrameID(pageID);
            logger.debug("get page " + pageID + " on frameID " + frameID);
            // TODO: ここも本当はpinが必要
            frames.get(frameID).pin();
            return frames.get(frameID);
        }
    }

    public void releasePage(int pageID){
        int frameID = this.pageTable.getFrameID(pageID);
        this.frames.get(frameID).unpin();
    }

    public Frame getNewPage() throws IOException {
        int pageID = this.pageDirectory.issueNewPage();
        // since new page is required only for write, we can always return empty frame.
        OptionalInt frameID = this.pageTable.getUnPinnedCleanFrameID(pageID);
        if (frameID.isEmpty()){
            // TODO: have to deal with concurrency problem and timing.
            this.evictPage();
            frameID = this.pageTable.getUnPinnedCleanFrameID(pageID);
        }
        System.out.println("then here?");
        Frame frame = this.frames.get(frameID.getAsInt());
        frame.clear();
        return frame;
    }

    private final ConcurrentHashMap<Integer, Lock> pageLocks = new ConcurrentHashMap<>();
    private Lock getLockForPage(int pageID) {
        return pageLocks.computeIfAbsent(pageID, id -> new ReentrantLock());
    }

    private void writeFrameToStorage(int pageID) throws IOException {
        logger.debug("writing page " + pageID + " on storage");
        int pageOffset = this.pageDirectory.getOffset(pageID);
        int frameID = this.pageTable.getFrameID(pageID);
        this.heapFile.writePage(pageOffset, this.frames.get(frameID));
        this.frames.get(frameID).clear();
        // cleanというよりnot used
    }

    private void evictPage() throws IOException {
        logger.debug("looking for frames to evict.");
        // TODO: implement eviction policy, which should be DIed in the end.
        // TODO: write corresponding WAL data on disk.
        // TODO: avoid busy wait.
        for (int i = 0; i< 5; i++){
            for (int frameID = 0; frameID < this.frames.size(); frameID++){
                // always supposed to be dirty
                if(!this.frames.get(frameID).pin()){
                    continue;
                }
                if(this.frames.get(frameID).isDirty()){
                    // ここでpageIDが-1になってしまう。frameIDを持つページが存在しないから。
                    int pageID = this.pageTable.getPageID(frameID);
                    logger.debug("evicting the frame " + frameID + " of page " + pageID);
                    this.writeFrameToStorage(pageID);
                    this.frames.get(frameID).clear();
                    this.pageTable.clearFrameAllocation(pageID);
                }else{
                    logger.debug("no eviction because frame" + frameID + " is clean");
                }
                this.frames.get(frameID).unpin();
                // ここでunpinしたせいでページを他の人にとられている。
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("all frames are pinned. you should abort query.");
    }

}
