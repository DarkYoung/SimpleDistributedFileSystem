package sdfs.client;

import sdfs.server.datanode.IDataNode;
import sdfs.server.filetree.BlockInfo;
import sdfs.server.filetree.FileNode;
import sdfs.server.filetree.LocatedBlock;
import sdfs.server.namenode.INameNode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {

    private static final long serialVersionUID = -2871677882327951658L;
    private final UUID uuid; // File uuid

    private final FileNode fileNode;

    private final boolean isReadOnly;

    private boolean isOpen;

    private long position; //读取或写入指针位置

    private final int BLOCK_SIZE = IDataNode.BLOCK_SIZE;
    private final int LOCATED_BLOCK_NUM = INameNode.LOCATED_BLOCK_NUM;
    private final String NAME_NODE_DATA_DIR = INameNode.NAME_NODE_DATA_DIR;

    private static NameNodeStub nameNodeStub = new NameNodeStub();
    private static DataNodeStub dataNodeStub = new DataNodeStub();

    private final transient byte[] byteBuffers; //缓冲区
    private long bufPos; //缓冲区指针位置

    public SDFSFileChannel(UUID uuid, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;
        this.byteBuffers = new byte[]{0};

        this.isOpen = true;
        this.position = 0;
        this.bufPos = 0;
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
        int lastBlockSize = (int) (fileSize % BLOCK_SIZE);

        int firstBlockIndex = (int) (position / BLOCK_SIZE);
        int offset = (int) (position % BLOCK_SIZE);

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
                if (readSize >= BLOCK_SIZE && infoIt.hasNext()) {
                    //不是最后一个块，并且buffer剩余空间足够放一个block的内容
                    dst.put(dataNodeStub.read(uuid, blockIt.next().getBlockNumber(), offset, BLOCK_SIZE));
                    readSize -= BLOCK_SIZE;
//                    position(position + BLOCK_SIZE);
                } else if (infoIt.hasNext()) {
                    //不是最后一个块，但buffer剩余空间不够放一个block的内容
                    dst.put(dataNodeStub.read(uuid, blockIt.next().getBlockNumber(), offset, readSize));
//                    readSize = 0;
//                    position(position + readSize);
                    break;
                } else {//最后一个块
                    int tmpSize = lastBlockSize > readSize ? readSize : lastBlockSize;
                    dst.put(dataNodeStub.read(uuid, blockIt.next().getBlockNumber(), offset, tmpSize));
                    readSize -= tmpSize;
//                    position(position + tmpSize);
                }
                offset = 0;
            }
        }
        position += realSize;
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
        //将数据写入到缓冲区
        byte[] srcBytes = src.array();
        System.arraycopy(srcBytes, 0, byteBuffers, (int) bufPos, srcBytes.length);

        //如果文件数据块不够，向nameNode申请新的空闲块
        //申请的空闲块数量：如果最后一个块的空闲空间小于src%BLOCK_SIZE，则需要多申请一个空闲块
        int newBlockNum = srcBytes.length / BLOCK_SIZE + ((position % BLOCK_SIZE + srcBytes.length % BLOCK_SIZE) < BLOCK_SIZE ? 0 : 1);
        for (int i = 0; i < newBlockNum; i++) {
            List<LocatedBlock> blocks = nameNodeStub.addBlocks(uuid, LOCATED_BLOCK_NUM);
            BlockInfo info = new BlockInfo();
            info.addLocatedBlocks(blocks);
            fileNode.addBlockInfo(info);
        }
        position += srcBytes.length;
        bufPos += srcBytes.length;
        setFileSize(position);
        return src.array().length;
    }

    /**
     * 将缓冲区内容（0～bufPos）写入磁盘
     *
     * @return
     */
    private void flushWrite() {
        if (bufPos <= 0)
            return;
        long oldPos = position - bufPos;
        int firstBlockIndex = (int) (oldPos / BLOCK_SIZE);
        int offset = (int) (oldPos % BLOCK_SIZE);

        int writeByte = 0;
        Iterator<LocatedBlock> blockIt;
        Iterator<BlockInfo> infoIt;
        for (infoIt = fileNode.iterator(); infoIt.hasNext(); ) {
            if (firstBlockIndex > 0) {
                infoIt.next();
                firstBlockIndex--;
                continue;
            }
            blockIt = infoIt.next().iterator();
            if (blockIt.hasNext() && offset != 0) { //这个block没装满 //最后一个块
                byte[] tmpBytes = Arrays.copyOfRange(byteBuffers, writeByte, BLOCK_SIZE - offset);
                dataNodeStub.write(uuid, blockIt.next().getBlockNumber(), offset, tmpBytes);
                writeByte += BLOCK_SIZE - offset;
                offset = 0;
            } else if (blockIt.hasNext()) {
                if (writeByte + BLOCK_SIZE < bufPos)
                    dataNodeStub.write(uuid, blockIt.next().getBlockNumber(), 0,
                            Arrays.copyOfRange(byteBuffers, writeByte, writeByte + BLOCK_SIZE));
                else dataNodeStub.write(uuid, blockIt.next().getBlockNumber(), 0,
                        Arrays.copyOfRange(byteBuffers, writeByte, (int) bufPos));
            }
        }
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
        if (newPosition < position)
            bufPos += newPosition - position;
        if (bufPos < 0)
            bufPos = 0;
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
        if (position > size) { //position大于size，则设position等于size
            bufPos += size - position;
            if (bufPos < 0)
                bufPos = 0;
            position = size;
        }
        setFileSize(size);
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
//        if (isReadOnly())
//            nameNodeStub.closeReadonlyFile(uuid);
//        else
//            nameNodeStub.closeReadwriteFile(uuid, (int) (position < fileNode.getFileSize() ? fileNode.getFileSize() : position));
        if (!isReadOnly()) //如果当前文件流是以读写方式打开的，则将缓冲区内容写入磁盘
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
        flushWrite();
        ObjectOutputStream outputStream =
                new ObjectOutputStream(new FileOutputStream(new File(NAME_NODE_DATA_DIR + getFileNodeId() + ".node")));
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
