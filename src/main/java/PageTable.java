import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

public class PageTable {

    private static final Logger logger = LogManager.getLogger(PageTable.class);
    private final Map<Integer, PageTableEntry> table = new HashMap<>();

    // TODO:これって後から変更したくないんだけどどう実装すればだっけ
    private int frameNum;

    PageTable(int frameNum){
        this.frameNum = frameNum;
    }

    boolean isPageLoaded(int pageID){
        return table.containsKey(pageID) && table.get(pageID).onMemory;
    }
    boolean isFrameDirty(int pageID){
        return table.get(pageID).isDirty();
    }
    void markPageClean(int pageID){
        table.computeIfPresent(pageID, (k, v) -> v.markClean());
    }

    // pageID -> entryで値を持っているので空いているframeを見つけるのが面倒。連続したフレームを見つけるのも大変。
    synchronized OptionalInt getFreeFrameID(int pageID){
        Set<Integer> usedFrameIDs = table.values().stream().filter(entry -> entry.isBeingUsed())
                .map(PageTableEntry::frameID)
                .collect(Collectors.toSet());
        for (int frameID = 0; frameID < this.frameNum; frameID++){
            if(!usedFrameIDs.contains(frameID)){
                // TODO: ここのfalseと外のclearを一致させないといけない。
                table.put(pageID, new PageTableEntry(false, true, frameID, true));
                logger.debug("returning " + frameID + " as a free frame.");
                return OptionalInt.of(frameID);
            }
        }
        return OptionalInt.empty();
    }

    int getFrameID(int pageID){
        return table.get(pageID).frameID();
    }

    void openFrame(int pageID){
        table.computeIfPresent(pageID, (k, v) -> v.withOnMemory(false));
    }

    public record PageTableEntry(boolean isDirty, boolean onMemory, int frameID, boolean isBeingUsed) {
        public PageTableEntry markClean() {
            return new PageTableEntry(false, onMemory, frameID, true);
        }

        public PageTableEntry withOnMemory(boolean onMemory) {
            return new PageTableEntry(isDirty, onMemory, frameID, true);
        }
    }

}
