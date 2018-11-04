package sdfs.filetree;

import sdfs.datanode.DataNode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileNode extends Node implements Iterable<BlockInfo> {


    private static final long serialVersionUID = 5124810810706823835L;

    /**
     * FileNode contains BlockInfos
     */
    public FileNode() {
        super(TYPE.FILE);
    }

    private final List<BlockInfo> blockInfoList = new ArrayList<>();

    private transient long fileSize; // file size should be checked when closing the file.

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
        int blockNum = fileSize % DataNode.BLOCK_SIZE > 0 ? 1 : 0;
        blockNum += fileSize / DataNode.BLOCK_SIZE;
        removeLastBlocks(getNumBlocks() - blockNum);
    }

    public int getNumBlocks() {
        return blockInfoList.size();
    }

    public void addBlockInfo(BlockInfo bi) {
        blockInfoList.add(bi);
    }

    public void addBlockInfos(List<BlockInfo> aList) {
        blockInfoList.addAll(aList);
    }

    public void removeLastBlocks(int blockAmount) {
        int size = blockInfoList.size();
        for (int i = size - 1; i >= size - blockAmount; i--) {
            blockInfoList.remove(i);
        }
    }

    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfoList.listIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfoList.equals(that.blockInfoList);
    }

    @Override
    public int hashCode() {
        return blockInfoList.hashCode();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeLong(fileSize);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        this.fileSize = ois.readLong();
    }
}

