package sdfs.server.filetree;


import java.io.Serializable;

public class LocatedBlock implements Serializable {

    private static final long serialVersionUID = 7904651623629246517L;
    private final int blockNumber;
    private final String host;
    public LocatedBlock(int blockNumber) {
        this.host = "localhost";
        this.blockNumber = blockNumber;
    }

    public LocatedBlock(String host, int blockNumber) {
        this.host = host;
        this.blockNumber = blockNumber
    }
    
    public String getHost() {
        return host;
    }
    
    public int getBlockNumber() {
        return blockNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocatedBlock that = (LocatedBlock) o;

        return host.equals(that.host) && blockNumber == that.blockNumber;
    }

    @Override
    public int hashCode() {
        return host.hasCode + blockNumber;
    }
}
