import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PageDirectory {

  private final Map<Integer, PageDirectoryEntry> directory = new HashMap<>();
  private final AtomicInteger maxPageID = new AtomicInteger(-1);

  private static final Logger logger = LogManager.getLogger(PageDirectory.class);

  int getOffset(int pageID) {
    return this.directory.get(pageID).getOffset();
  }

  boolean isPageNew(int pageID) {
    return this.directory.containsKey(pageID);
  }

  int issueNewPage() {
    int pageID = maxPageID.incrementAndGet();
    logger.debug("issued new pageID " + pageID);
    // TODO: avoid magic number. unify the variavle with the on on BufferPool.
    // TODO: let's say that page will be saved sequentially now.
    this.directory.put(pageID, new PageDirectoryEntry(pageID * 4096));
    return pageID;
  }

  public record PageDirectoryEntry(int offset) {
    public int getOffset() {
      return offset;
    }
  }
}
