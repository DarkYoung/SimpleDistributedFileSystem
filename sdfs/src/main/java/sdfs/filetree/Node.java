package sdfs.filetree;


import java.io.Serializable;

public abstract class Node implements Serializable {
    private static final long serialVersionUID = -4747065725721371736L;
    // TODO your code here

    private static long ID = 0;
    private long nodeId;

    public enum TYPE {
        FILE, DIR
    }

    private TYPE type;

    Node(TYPE type) {
        this.type = type;
        this.nodeId = ID++;
    }

    TYPE getType() {
        return this.type;
    }

    public long getNodeId() {
        return nodeId;
    }

    Node getNodeById(long id) {
        return null;
    }


}