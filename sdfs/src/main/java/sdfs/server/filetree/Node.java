package sdfs.server.filetree;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public abstract class Node implements Serializable {
    private static final long serialVersionUID = -5698868247971868199L;

    private static long ID = 0; //一个静态变量无论是否被transient修饰，都不能被序列化
    private transient long nodeId;

    public enum TYPE {
        FILE, DIR
    }

    private transient TYPE type;

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

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeLong(nodeId);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        this.nodeId = ois.readLong();
    }

}