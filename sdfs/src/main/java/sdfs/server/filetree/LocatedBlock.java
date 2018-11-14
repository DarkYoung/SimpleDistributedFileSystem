package sdfs.server.filetree;


import java.io.Serializable;

public class LocatedBlock implements Serializable {
    private final int blockNumber;
    private final String host;
    private final int port;

    public LocatedBlock(int blockNumber) {
        this("", blockNumber);
    }

    /**
     * 构造方法
     *
     * @param host        LocatedBlock可能由不同的DataNode管理，host指不同DataNode对应的ip
     * @param blockNumber
     */
    public LocatedBlock(String host, int blockNumber) {
        this(host, 0, blockNumber);
    }

    /**
     * 构造方法
     *
     * @param host        LocatedBlock可能由不同的DataNode管理，host指不同DataNode对应的ip
     * @param port        针对同一主机部署多个服务器（DataNode）的情况，以不同的DataNode监听不同端口来区分服务器
     * @param blockNumber
     */
    public LocatedBlock(String host, int port, int blockNumber) {
        this.host = host;
        this.port = port;
        this.blockNumber = blockNumber;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocatedBlock that = (LocatedBlock) o;

        return host.equals(that.host) && port == that.port && blockNumber == that.blockNumber;
    }

    @Override
    public int hashCode() {
        return (host + port + blockNumber).hashCode();
    }
}
