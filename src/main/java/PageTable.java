import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PageTable {

    private static final Logger logger = LogManager.getLogger(PageTable.class);
    private final ConcurrentHashMap<Integer, Frame> table = new ConcurrentHashMap<>();

    private final List<Frame> frames;

    boolean hasEvicted(int pageID) {
        return this.table.get(pageID) == null;
    }

    PageTable(List<Frame> frames){
        this.frames = frames;
    }

    boolean isFrameDirty(int pageID){
        return table.get(pageID).isDirty();
    }

    // pageID -> entryで値を持っているので空いているframeを見つけるのが面倒。連続したフレームを見つけるのも大変。
     OptionalInt getUnPinnedCleanFrameID(int pageID){
        logger.debug("looking for frame to give page " + pageID);
        for (int frameID = 0; frameID < this.frames.size(); frameID++){
            if(!frames.get(frameID).isDirty() && frames.get(frameID).pin()){
                // TODO: ここのfalseと外のclearを一致させないといけない。
                table.put(pageID, frames.get(frameID));
                logger.debug("giving away frameID " + frameID + " to page " + pageID);
                return OptionalInt.of(frameID);
            }
        }
        logger.debug("no free frame was found.");
        return OptionalInt.empty();
    }

    void clearFrameAllocation(int pageID){
        this.table.computeIfPresent(pageID, (k, v) -> null);
    }
    int getFrameID(int pageID){
        return table.get(pageID).getID();
    }


    
    int getPageID(int frameID){
        // TODO: refactor
        return table.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().getID() == frameID)
                .map(Map.Entry::getKey)
                .findAny()
                .orElse(-1);
    }


}
