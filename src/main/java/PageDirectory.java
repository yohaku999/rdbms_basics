import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PageDirectory {

    private static final Map<Integer, PageDirectoryEntry> directory = new HashMap<>();
    private static final AtomicInteger maxPageID = new AtomicInteger(-1);

    private static final Logger logger = LogManager.getLogger(PageDirectory.class);
    int getOffset(int pageID){
        return PageDirectory.directory.get(pageID).getOffset();
    }



    boolean isPageNew(int pageID){
        return PageDirectory.directory.containsKey(pageID);
    }

    int issueNewPage(){
        int pageID = maxPageID.incrementAndGet();
        logger.debug("issued new pageID " + pageID);
        // TODO: avoid magic number. unify the variavle with the on on BufferPool.
        // TODO: let's say that page will be saved sequentially now.
        PageDirectory.directory.put(pageID, new PageDirectoryEntry(pageID*4096));
        return pageID;
    }

    public record PageDirectoryEntry(int offset) {
        public int getOffset() {
            return offset;
        }
    }


}
