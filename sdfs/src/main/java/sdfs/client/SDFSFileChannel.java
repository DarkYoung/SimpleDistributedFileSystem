package sdfs.client;

import sdfs.datanode.DataNode;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;
import sdfs.filetree.LocatedBlock;
import sdfs.namenode.NameNode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

public class SDFSFileChannel implements SeekableByteChannel, Flushable {

    private final UUID uuid; // File uuid

    private final FileNode fileNode;

    private final boolean isReadOnly;

    private boolean isOpen;

    private long position;

    private static NameNode nameNode = NameNode.getInstance();
    private static DataNode dataNode = DataNode.getInstance();


    public SDFSFileChannel(UUID uuid, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;

        // TODO your code here
        isOpen = true;
        position = 0;
    }


    @Override
    public int read(ByteBuffer dst) throws IOException {
        // TODO your code here
        if (fileNode == null || fileNode.getFileSize() - position <= 0)
            return -1;
        long fileSize = fileNode.getFileSize();
        int bufSize = dst.limit() - dst.position();
        int readSize = bufSize > fileSize ? (int) fileSize : bufSize;
        int realSize = readSize;
        int lastBlockSize = (int) (fileSize % DataNode.BLOCK_SIZE);

        int firstBlockIndex = (int) (position / DataNode.BLOCK_SIZE);
        int offset = (int) (position % DataNode.BLOCK_SIZE);

        Iterator<LocatedBlock> blockIt;
        Iterator<BlockInfo> infoIt;
        for (infoIt = fileNode.iterator(); infoIt.hasNext(); ) {
            if (firstBlockIndex > 0) {
                infoIt.next();
                firstBlockIndex--;
                continue;
            }
            blockIt = infoIt.next().iterator();
            if (blockIt.hasNext()) {
                if (readSize >= DataNode.BLOCK_SIZE && infoIt.hasNext()) {
                    //不是最后一个块，并且buffer剩余空间足够放一个block的内容
                    dst.put(dataNode.read(uuid, blockIt.next().getBlockNumber(), offset, DataNode.BLOCK_SIZE));
                    readSize -= DataNode.BLOCK_SIZE;
//                    position(position + DataNode.BLOCK_SIZE);
                } else if (infoIt.hasNext()) {
                    //不是最后一个块，但buffer剩余空间不够放一个block的内容
                    dst.put(dataNode.read(uuid, blockIt.next().getBlockNumber(), offset, readSize));
//                    readSize = 0;
//                    position(position + readSize);
                    break;
                } else if (!infoIt.hasNext()) {//最后一个块
                    int tmpSize = lastBlockSize > readSize ? readSize : lastBlockSize;
                    dst.put(dataNode.read(uuid, blockIt.next().getBlockNumber(), offset, tmpSize));
                    readSize -= tmpSize;
//                    position(position + tmpSize);
                }
                offset = 0;
            }
        }
        position(position + realSize);
        return realSize;
    }


    @Override
    public int write(ByteBuffer src) throws IOException {
        // TODO your code here
        if (fileNode == null)
            return 0;
        byte[] srcBytes = src.array();

        int firstBlockIndex = (int) (position / DataNode.BLOCK_SIZE);
        int offset = (int) (position % DataNode.BLOCK_SIZE);

        Iterator<LocatedBlock> blockIt;
        Iterator<BlockInfo> infoIt;
        int count = 0;
        for (infoIt = fileNode.iterator(); infoIt.hasNext(); count++) {
            if (firstBlockIndex > 0) {
                infoIt.next();
                firstBlockIndex--;
                continue;
            }
            blockIt = infoIt.next().iterator();
            if (blockIt.hasNext() && offset != 0) { //这个block没装满 //最后一个块
                byte[] tmpBytes = Arrays.copyOfRange(srcBytes, 0, DataNode.BLOCK_SIZE - offset);
                dataNode.write(uuid, blockIt.next().getBlockNumber(), offset, tmpBytes);
                srcBytes = Arrays.copyOfRange(srcBytes, DataNode.BLOCK_SIZE - offset, srcBytes.length);
                firstBlockIndex += 1;
                position(position + DataNode.BLOCK_SIZE - offset);
                if (infoIt.hasNext()) //覆盖指针后的块（删除）
                    nameNode.removeLastBlocks(uuid, fileNode.getNumBlocks() - count);

            }
        }
//        ArrayList<BlockInfo> infos = new ArrayList<>();
        int lastBlockSize = srcBytes.length % DataNode.BLOCK_SIZE;
        int blockNums = srcBytes.length / DataNode.BLOCK_SIZE + (lastBlockSize > 0 ? 1 : 0);
        for (int i = 0; i < blockNums; i++) {
            byte[] tmpBytes = Arrays.copyOfRange(srcBytes, i * DataNode.BLOCK_SIZE,
                    i * DataNode.BLOCK_SIZE + (i == (blockNums - 1) ? lastBlockSize : DataNode.BLOCK_SIZE));
            List<LocatedBlock> blocks = nameNode.addBlocks(uuid, NameNode.LOCATED_BLOCK_NUM);
            for (LocatedBlock block : blocks) {
                dataNode.write(uuid, block.getBlockNumber(), 0, tmpBytes);
            }
//            BlockInfo info = new BlockInfo();.
//            info.addLocatedBlocks(blocks);
//            infos.add(info);
        }
//        fileNode.addBlockInfos(infos);
        position(position + srcBytes.length);
        setFileSize(position);
        return srcBytes.length;
    }


    @Override
    public long position() throws IOException {
        // TODO your code here
        if (!isOpen())
            throw new ClosedChannelException();
        return position;
    }


    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        // TODO your code here
        if (!isOpen())
            throw new ClosedChannelException();
        if (newPosition < 0)
            throw new IllegalArgumentException();
        position = newPosition;
        return this;
    }


    @Override
    public long size() throws IOException {
        // TODO your code here
        if (!isOpen())
            throw new ClosedChannelException();
        return fileNode.getFileSize();
    }


    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        // TODO your code here
        if (isReadOnly())
            throw new NonWritableChannelException();
        if (!isOpen())
            throw new ClosedChannelException();
        if (size < 0)
            throw new IllegalArgumentException();

        long fileSize = fileNode.getFileSize();
        if (fileSize <= size) //不做改变
            return this;
        if (position > size)
            position = size;
        setFileSize(size);
        nameNode.removeLastBlocks(uuid, (int) (fileSize / DataNode.BLOCK_SIZE - size / DataNode.BLOCK_SIZE));
        return this;
    }

    @Override
    public boolean isOpen() {
        // TODO your code here
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        // TODO your code here
        if (!isOpen())
            return;
        isOpen = false;
        flush();
    }

    /**
     * Flushes this stream by writing any buffered output to the underlying
     * stream.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void flush() throws IOException {
        // TODO your code here
        ObjectOutputStream outputStream =
                new ObjectOutputStream(new FileOutputStream(new File(NameNode.NAMENODE_DATA_DIR + getFileNodeId() + ".node")));
        outputStream.writeObject(fileNode);
        outputStream.flush();
        outputStream.close();
    }

    /**
     * Is the file channel read-only?
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Get UUID of the file channel
     *
     * @return uuid
     */
    public UUID getUuid() {
        return uuid;
    }

    /**
     * Get the amount of blocks in this file
     *
     * @return the amount of the blocks
     */
    public int getNumBlocks() {
        return fileNode.getNumBlocks();
    }

    /**
     * Set the size of this file channel
     *
     * @param fileSize new size
     */
    public void setFileSize(long fileSize) {
        this.fileNode.setFileSize(fileSize);
    }

    public FileNode getFileNode() {
        return fileNode;
    }

    public long getFileNodeId() {
        return fileNode.getNodeId();
    }
}
