import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class BufferPoolTest {

    private BufferPool bfp;
    private int PAGE_SIZE = 4096;

    @BeforeEach
    void setUp() {
        bfp = new BufferPool(PAGE_SIZE*8, new HeapFile());
    }

    @Test
    // ensure it gets empty frame.
    void testGetNewPage() throws IOException {
        Frame frame = bfp.getNewPage();
        for (int i = 0; i < frame.getBuffer().limit(); i++) {
            assertEquals(0, frame.getBuffer().get(i));
        }
    }

    @Test
    void testWritePageToStorage() throws IOException {
        List<Frame> frames = new ArrayList<>();
        for(int pageID = 0; pageID < 3; pageID++){
            // create random bytes
            byte[] bytes = new byte[PAGE_SIZE];
            Random random = new Random();
            random.nextBytes(bytes);

            // convert to byte buffer
            // TODO: strange that I cannnot collect pageID from anywhare.
            Frame frame = bfp.getNewPage();
            frame.write(bytes);
            frames.add(frame);
        }

        for(int pageID = 0; pageID < 3; pageID++){
            Frame readBuffer = bfp.getPage(pageID);
            for (int i = 0; i < frames.get(pageID).getBuffer().limit(); i++) {
                assertEquals(frames.get(pageID).getBuffer().get(i), readBuffer.getBuffer().get(i));
            }
        }
    }

    @Test
    void testConcurrentWritePageToStorage() throws InterruptedException {
        int numPages = 5;
        List<Frame> frames = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(numPages);
        try{
            // write
            List<Callable<Void>> writeTasks = new ArrayList<>();
            for (int pageID = 0; pageID < numPages; pageID++) {
                writeTasks.add(() -> {
                    byte[] bytes = new byte[PAGE_SIZE];
                    new Random().nextBytes(bytes);
                    Frame frame = bfp.getNewPage();
                    frame.write(bytes);
                    synchronized (frames) {
                        frames.add(frame);
                    }
                    return null;
                });
            }
            // 書き込みを並列実行
            executor.invokeAll(writeTasks);

            // 2. 読み込みタスク
            List<Callable<Void>> readTasks = new ArrayList<>();
            for (int pageID = 0; pageID < numPages; pageID++) {
                final int pid = pageID;
                readTasks.add(() -> {
                    Frame frame = bfp.getPage(pid);
                    // byte buffer should be compared this way for comparing data.
                    for (int i = 0; i < frames.get(pid).getBuffer().limit(); i++) {
                        assertEquals(frames.get(pid).getBuffer().get(i), frame.getBuffer().get(i));
                    }
                    return null;
                });
            }
            // 読み込みを並列実行
            executor.invokeAll(readTasks);

        }finally {
                executor.shutdown();
        }
    }

    @Test
    void testPageEvictionAndRead() throws InterruptedException {
        // 2 pages should be evicted, since there are only 8 frames in buffer pool.
        int numPages = 10;
        List<Frame> frames = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(numPages);
        try{
            // write
            List<Callable<Void>> writeTasks = new ArrayList<>();
            for (int pageID = 0; pageID < numPages; pageID++) {
                writeTasks.add(() -> {
                    byte[] bytes = new byte[PAGE_SIZE];
                    new Random().nextBytes(bytes);
                    Frame frame = bfp.getNewPage();
                    frame.write(bytes);
                    synchronized (frames) {
                        frames.add(frame);
                    }
                    return null;
                });
            }
            // 書き込みを並列実行
            executor.invokeAll(writeTasks);

            // 2. 読み込みタスク
            List<Callable<Void>> readTasks = new ArrayList<>();
            for (int pageID = 0; pageID < numPages; pageID++) {
                final int pid = pageID;
                readTasks.add(() -> {
                    Frame readBuffer = bfp.getPage(pid);
                    // byte buffer should be compared this way for comparing data.
                    for (int i = 0; i < frames.get(pid).getBuffer().limit(); i++) {
                        assertEquals(frames.get(pid).getBuffer().get(i), readBuffer.getBuffer().get(i));
                    }
                    return null;
                });
            }
            // 読み込みを並列実行
            executor.invokeAll(readTasks);

        }finally {
            executor.shutdown();
        }

    }

}
