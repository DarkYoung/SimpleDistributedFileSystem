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

    private long position; //读取或写入指针位置

    private static NameNodeStub nameNodeStub = new NameNodeStub();
    private static DataNodeStub dataNodeStub = new DataNodeStub();


    public SDFSFileChannel(UUID uuid, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;

        isOpen = true;
        position = 0;
    }

    /**
     * 从channel读取bytes到dst
     * <p>
     * 从当前读取指针位置开始读取
     * 更新当前指针到实际读取到的位置
     *
     * @return 实际读取的字节数，失败返回-1
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!isOpen()) //文件流处于打开状态
            throw new ClosedChannelException();

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
                    dst.put(dataNodeStub.read(uuid, blockIt.next().getBlockNumber(), offset, DataNode.BLOCK_SIZE));
                    readSize -= DataNode.BLOCK_SIZE;
//                    position(position + DataNode.BLOCK_SIZE);
                } else if (infoIt.hasNext()) {
                    //不是最后一个块，但buffer剩余空间不够放一个block的内容
                    dst.put(dataNodeStub.read(uuid, blockIt.next().getBlockNumber(), offset, readSize));
//                    readSize = 0;
//                    position(position + readSize);
                    break;
                } else if (!infoIt.hasNext()) {//最后一个块
                    int tmpSize = lastBlockSize > readSize ? readSize : lastBlockSize;
                    dst.put(dataNodeStub.read(uuid, blockIt.next().getBlockNumber(), offset, tmpSize));
                    readSize -= tmpSize;
//                    position(position + tmpSize);
                }
                offset = 0;
            }
        }
        position(position + realSize);
        return realSize;
    }

    /**
     * 将src包含的字节写入channel
     * <p>
     * 从当前指针处开始写入字节
     * 更新当前指针到最后写入的位置
     *
     * @return 实际写入的字节数，失败返回-1
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!isOpen()) //文件流处于打开状态
            throw new ClosedChannelException();

        if (fileNode == null)
            return -1;
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
                dataNodeStub.write(uuid, blockIt.next().getBlockNumber(), offset, tmpBytes);
                srcBytes = Arrays.copyOfRange(srcBytes, DataNode.BLOCK_SIZE - offset, srcBytes.length);
                firstBlockIndex += 1;
                position(position + DataNode.BLOCK_SIZE - offset);
                if (infoIt.hasNext()) //覆盖指针后的块（删除）
                    nameNodeStub.removeLastBlocks(uuid, fileNode.getNumBlocks() - count);

            }
        }
//        ArrayList<BlockInfo> infos = new ArrayList<>();
        int lastBlockSize = srcBytes.length % DataNode.BLOCK_SIZE;
        int blockNums = srcBytes.length / DataNode.BLOCK_SIZE + (lastBlockSize > 0 ? 1 : 0);
        for (int i = 0; i < blockNums; i++) {
            byte[] tmpBytes = Arrays.copyOfRange(srcBytes, i * DataNode.BLOCK_SIZE,
                    i * DataNode.BLOCK_SIZE + (i == (blockNums - 1) ? lastBlockSize : DataNode.BLOCK_SIZE));
            List<LocatedBlock> blocks = nameNodeStub.addBlocks(uuid, NameNode.LOCATED_BLOCK_NUM);
            for (LocatedBlock block : blocks) {
                dataNodeStub.write(uuid, block.getBlockNumber(), 0, tmpBytes);
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


    /**
     * @return channel的当前读写指针位置
     */
    @Override
    public long position() throws IOException {
        if (!isOpen()) //文件流处于打开状态
            throw new ClosedChannelException();
        return position;
    }


    /**
     * 设置channel的当前读写位置position
     *
     * @return this
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (!isOpen()) //文件流处于打开状态
            throw new ClosedChannelException();
        if (newPosition < 0) //非负数
            throw new IllegalArgumentException();
        position = newPosition;
        return this;
    }


    /**
     * @return 当前channel连接到的文件大小
     */
    @Override
    public long size() throws IOException {
        if (!isOpen()) //文件流处于打开状态
            throw new ClosedChannelException();
        return fileNode.getFileSize();
    }


    /**
     * truncate：指掐头或去尾
     * <p>
     * 如果给定size小于文件大小，则去掉文件大于size部分的内容
     * 如果size大于或等于文件大小，则不做改变
     * 如果当前position大于size，则设position等于size
     *
     * @return this
     */
    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        if (isReadOnly()) //只读文件流不允许truncate
            throw new NonWritableChannelException();
        if (!isOpen()) //channel应当处于打开状态
            throw new ClosedChannelException();
        if (size < 0)
            throw new IllegalArgumentException();

        long fileSize = fileNode.getFileSize();
        if (fileSize <= size) //不做改变
            return this;
        if (position > size) //position大于size，则设position等于size
            position = size;
        setFileSize(size);
        nameNodeStub.removeLastBlocks(uuid, (int) (fileSize / DataNode.BLOCK_SIZE - size / DataNode.BLOCK_SIZE));
        return this;
    }

    /**
     * @return channel是否打开
     */
    @Override
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * 关闭channel
     * 将缓冲区内容写入磁盘
     */
    @Override
    public void close() throws IOException {
        if (!isOpen()) //channel应当处于打开状态
            return;
        isOpen = false;
        if (isReadOnly())
            nameNodeStub.closeReadonlyFile(uuid);
        else
            nameNodeStub.closeReadwriteFile(uuid, (int) (position < fileNode.getFileSize() ? fileNode.getFileSize() : position));
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
        if (fileSize < 0)
            return;
        this.fileNode.setFileSize(fileSize);
    }

    public FileNode getFileNode() {
        return fileNode;
    }

    public long getFileNodeId() {
        return fileNode.getNodeId();
    }
}
