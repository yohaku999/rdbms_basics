import java.util.HashMap;
import java.util.Map;

public class PageTable {

    private static final Map<Integer, PageTableEntry> table = new HashMap<>();

    boolean isPageLoaded(int pageID){
        return table.containsKey(pageID);
    }

    // TODO: concurrency problem.
    int getFreeFrameID(){
        return 0;
    }

    int getFrameOffset(int pageID){
        return PageTable.table.get(pageID).getOffset();
    }

    void afterPageLoad(int pageID, int offset){
        PageTableEntry entry = new PageTableEntry();
        entry.offset = offset;
        PageTable.table.put(pageID, entry);
    }

    class PageTableEntry{
        private int offset;

        public int getOffset() {
            return offset;
        }
    }


}
