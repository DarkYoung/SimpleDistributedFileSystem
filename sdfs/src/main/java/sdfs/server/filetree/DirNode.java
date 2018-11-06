package sdfs.server.filetree;

import sdfs.util.FileUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirNode extends Node implements Iterable<Entry> {

    private static final long serialVersionUID = -2976646025698460216L;
    /**
     * DirNode has entries in it
     */
    private final Set<Entry> entries = new HashSet<>();

    public DirNode() {
        super(TYPE.DIR);
    }

    public void addEntry(String name, Node node) {
        Entry entry = new Entry(name, node);
        entries.add(entry);
    }

    public Node lookUp(String fileUri, TYPE type) {
        fileUri = fileUri.trim();
        if ((type == TYPE.FILE && FileUtil.isValidPath(fileUri)) || (type == TYPE.DIR && FileUtil.isValidDir(fileUri))) {
            if (fileUri.equals("/"))
                return this;
            String tokens[] = fileUri.substring(1).split("/");
            DirNode node = this;
            for (int i = 0; i < tokens.length - 1; i++) {
                if (node == null)
                    return null;
                else
                    node = (DirNode) node.getNodeByName(tokens[i]);
            }
            if (node != null)
                return node.getNodeByName(tokens[tokens.length - 1]);
        }
        return null;
    }

    private Node getNodeByName(String name) {
        for (Entry entry : this) {
            if (entry.getName().equals(name)) {
                Node node = null;
                try {
                    ObjectInputStream inputStream =
                            new ObjectInputStream(new FileInputStream(new File(
                                    System.getProperty("sdfs.namenode.dir") + "/" + entry.getNode().getNodeId() + ".node")));

                    node = (Node) inputStream.readObject();
                } catch (IOException | ClassNotFoundException ignored) {

                }
                return node;
            }
        }
        return null;
    }

    @Override
    public Iterator<Entry> iterator() {
        return entries.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DirNode that = (DirNode) o;

        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
