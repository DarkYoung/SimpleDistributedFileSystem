package sdfs.filetree;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Entry implements Serializable {

    private static final long serialVersionUID = -1256556490076421777L;
    private transient Node node;

    private transient String name;

    public Entry(String name, Node node) {
        if (name == null || node == null) {
            throw new NullPointerException();
        }
        if (name.isEmpty() || name.contains("/")) {
            throw new IllegalArgumentException();
        }
        this.name = name;
        this.node = node;
    }

    public Node getNode() {
        return node;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entry entry = (Entry) o;

        return name.equals(entry.name);

    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(node);
        oos.writeUTF(name);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        this.node = (Node) ois.readObject();
        this.name = ois.readUTF();
    }
}
