import org.junit.jupiter.api.BeforeEach;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class BufferPoolTest {

    private BufferPool bfp;

    @BeforeEach
    void setUp() {
        bfp = new BufferPool(4096*25*10*500);
    }

    @Test
    void testGetNewPage() {
        ByteBuffer frame = bfp.getNewPage();
        for (int i = 0; i < frame.limit(); i++) {
            assertEquals(0, frame.get(i));
        }
    }

    @Test
    void testWritePageToStorage(){
        int page_size = 4096;
        List<ByteBuffer> frames = new ArrayList<>();
        for(int pageID = 0; pageID < 5; pageID++){
            // create random bytes
            byte[] bytes = new byte[page_size];
            Random random = new Random();
            random.nextBytes(bytes);

            // convert to byte buffer
            // TODO: strange that I cannnot collect pageID from anywhare.
            ByteBuffer frame = bfp.getNewPage();
            frame.put(bytes);
            frames.add(frame);
        }

        for(int pageID = 0; pageID < 5; pageID++){
            ByteBuffer readBuffer = bfp.getPage(pageID);
            for (int i = 0; i < frames.get(pageID).remaining(); i++) {
                assertEquals(frames.get(pageID).get(i), readBuffer.get(i));
            }
        }
    }

    @Test
    void testConcurrentWritePageToStorage() throws InterruptedException {
        int page_size = 4096;
        int numPages = 2;
        List<ByteBuffer> frames = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(numPages);
        try{
        // write
        List<Callable<Void>> writeTasks = new ArrayList<>();
        for (int pageID = 0; pageID < numPages; pageID++) {
            writeTasks.add(() -> {
                byte[] bytes = new byte[page_size];
                new Random().nextBytes(bytes);
                ByteBuffer frame = bfp.getNewPage();
                frame.put(bytes);
                synchronized (frames) {
                    frames.add(frame);
                }
                return null;
            });
        }
        System.out.println("here");
        // 書き込みを並列実行
        executor.invokeAll(writeTasks);

        // 2. 読み込みタスク
        List<Callable<Void>> readTasks = new ArrayList<>();
        for (int pageID = 0; pageID < numPages; pageID++) {
            final int pid = pageID;
            readTasks.add(() -> {
                ByteBuffer readBuffer = bfp.getPage(pid);
                // byte buffer should be compared this way for comparing data.
                for (int i = 0; i < frames.get(pid).remaining(); i++) {
                    assertEquals(frames.get(pid).get(i), readBuffer.get(i));
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
