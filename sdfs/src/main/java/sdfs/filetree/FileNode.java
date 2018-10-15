package sdfs.filetree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileNode extends Node implements Iterable<BlockInfo> {

    private static final long serialVersionUID = 3189629476629266917L;

    /**
     * FileNode contains BlockInfos
     */
    public FileNode() {
        super(TYPE.FILE);
    }

    private final List<BlockInfo> blockInfoList = new ArrayList<>();

    private long fileSize; // file size should be checked when closing the file.

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
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
}

