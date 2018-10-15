package sdfs.filetree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BlockInfo implements Iterable<LocatedBlock>, Serializable {

    private static final long serialVersionUID = 2256744734112432185L;
    /**
     * BlockInfo stores a list of LocatedBlocks with each of them contains the address and ID of a replica of the block.
     */

    private final List<LocatedBlock> locatedBlocks = new ArrayList<>();


    @Override
    public Iterator<LocatedBlock> iterator() {
        return locatedBlocks.iterator();
    }

    public void addLocatedBlocks(List<LocatedBlock> blocks) {
        locatedBlocks.addAll(blocks);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockInfo that = (BlockInfo) o;

        return locatedBlocks.equals(that.locatedBlocks);
    }

    @Override
    public int hashCode() {
        return locatedBlocks.hashCode();
    }
}
