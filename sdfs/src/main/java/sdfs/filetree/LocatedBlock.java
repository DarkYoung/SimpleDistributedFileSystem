package sdfs.filetree;


public class LocatedBlock{

    private final int blockNumber;

    public LocatedBlock(int blockNumber) {
        this.blockNumber = blockNumber;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocatedBlock that = (LocatedBlock) o;

        return blockNumber == that.blockNumber;
    }

    @Override
    public int hashCode() {
        return blockNumber;
    }
}
