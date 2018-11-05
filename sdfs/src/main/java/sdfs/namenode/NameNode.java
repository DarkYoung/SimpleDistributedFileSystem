package sdfs.namenode;

import sdfs.*;
import sdfs.client.SDFSFileChannel;
import sdfs.datanode.DataNode;
import sdfs.filetree.*;
import sdfs.util.FileUtil;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;
import java.util.*;


public class NameNode extends AbstractServer implements INameNode {
    private Map<UUID, SDFSFileChannel> channels;
    private DirNode rootNode;
    private static String rootNodePath = NAME_NODE_DATA_DIR + "0.node";
    private static int blockId = 0;
    private int port;

    /**
     * SDFSFileChannel包括对应FileNode信息
     * 每个FileNode包括多个BlockInfo
     * 根据BlockInfo对象可以寻找到单个块对应的所有备份，即LocatedBlock对象列表
     * 根据不同的LocatedBlock中的blockNumber寻找到对应的DataNode节点上的数据块
     */
    public NameNode() {
        channels = new HashMap<>();
        try {
            ObjectInputStream inputStream =
                    new ObjectInputStream(new FileInputStream(new File(rootNodePath)));
            rootNode = (DirNode) inputStream.readObject();
        } catch (FileNotFoundException e) {
            rootNode = new DirNode();
            try {
                ObjectOutputStream outputStream =
                        new ObjectOutputStream(new FileOutputStream(new File(rootNodePath)));
                outputStream.writeObject(rootNode);
            } catch (IOException ignored) {

            }
        } catch (IOException ignored) {
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        //默认主机：localhost，默认端口：port
        register("localhost", Constants.DEFAULT_PORT);
    }

    /**
     * 将所有public函数注册到注册中心，只有注册的函数才能被远程调用
     */
    public void register(String host, int port) {
        this.port = port;
        Registry.register(new Url(host, port, getClass().getName()));

    }

    /* listening requests from client */
    public void listenRequest() {
        listenRequest(port);
    }

    /**
     * 以”只读“方式打开一个文件流
     *
     * @param fileUri The file uri to be open
     * @return SDFSFileChannel Instance
     */
    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws InvalidPathException, FileNotFoundException {
        if (fileUri == null || fileUri.isEmpty())
            throw new InvalidPathException("", "");
        FileNode fileNode = (FileNode) rootNode.lookUp(fileUri, Node.TYPE.FILE);
        if (fileNode != null) {
            SDFSFileChannel channel = new SDFSFileChannel(UUID.randomUUID(), fileNode, true);
            channels.put(channel.getUuid(), channel);
            return channel;
        }
        throw new FileNotFoundException();
    }


    /**
     * 以”读写“方式打开一个文件流
     *
     * @param fileUri The file uri to be open
     */
    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws InvalidPathException, FileNotFoundException, IllegalStateException {
        FileNode fileNode = (FileNode) rootNode.lookUp(fileUri, Node.TYPE.FILE);
        if (fileNode != null) {
            if (!readWriteTwice(fileNode)) {
                SDFSFileChannel channel = new SDFSFileChannel(UUID.randomUUID(), fileNode, false);
                channels.put(channel.getUuid(), channel);
                return channel;
            } else throw new IllegalStateException();
        }
        throw new FileNotFoundException();
    }

    /**
     * 新建一个文件
     * 如果父目录不存在，则先创建父目录
     *
     * @param fileUri The file uri to be create
     */
    @Override
    public SDFSFileChannel create(String fileUri) throws FileAlreadyExistsException, InvalidPathException {
        if (rootNode.lookUp(fileUri, Node.TYPE.FILE) != null)
            throw new FileAlreadyExistsException(fileUri);
        if (FileUtil.isValidPath(fileUri)) {
            String parentPath = fileUri.substring(0, fileUri.lastIndexOf("/") + 1);
            DirNode dirNode = (DirNode) rootNode.lookUp(parentPath, Node.TYPE.DIR);
            if (dirNode == null) {
                mkdir(parentPath);
                dirNode = (DirNode) rootNode.lookUp(parentPath, Node.TYPE.DIR);
            }
            FileNode fileNode = new FileNode();
            addNode(fileNode);
            dirNode.addEntry(FileUtil.getName(fileUri), fileNode);
            addNode(dirNode);
            SDFSFileChannel channel = new SDFSFileChannel(UUID.randomUUID(), fileNode, false);
            channels.put(channel.getUuid(), channel);
            return channel;
        }
        return null;
    }

    /**
     * 通过唯一标识码fileUuid打开一个已存在的可”读“文件流
     *
     * @param fileUuid the file uuid with readonly state
     */
    @Override
    public SDFSFileChannel getReadonlyFile(UUID fileUuid) throws IllegalStateException {
        SDFSFileChannel channel = channels.get(fileUuid);
        if (channel == null || !channel.isReadOnly())
            throw new IllegalStateException();
        return channel;

    }

    /**
     * 通过唯一标识码fileUuid打开一个已存在的可”读写“文件流
     *
     * @param fileUuid the file uuid with readwrite state
     */
    @Override
    public SDFSFileChannel getReadwriteFile(UUID fileUuid) throws IllegalStateException {
        SDFSFileChannel channel = channels.get(fileUuid);
        if (channel == null || channel.isReadOnly())
            throw new IllegalStateException();
        return channel;
    }

    /**
     * 关闭一个“可读”文件流
     *
     * @param fileUuid file to be closed
     */
    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException {
//        SDFSFileChannel channel = channels.get(fileUuid);
//        channel.close();
        if (fileUuid == null || !channels.containsKey(fileUuid))
            throw new IllegalStateException();
        channels.remove(fileUuid);
    }

    /**
     * 关闭一个可“读写”文件流
     *
     * @param fileUuid    file to be closed
     * @param newFileSize The new file size after modify
     */
    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException {
//        SDFSFileChannel channel = channels.get(fileUuid);
//        channel.close();
        if (fileUuid == null || !channels.containsKey(fileUuid))
            throw new IllegalStateException();
        SDFSFileChannel channel = channels.get(fileUuid);
        //文件大小应当处于（(blockAmount - 1) * BLOCK_SIZE, blockAmount * BLOCK_SIZE)，即最后一个块可能没装满
        if (newFileSize < (channel.getNumBlocks() - 1) * DataNode.BLOCK_SIZE
                || newFileSize > channel.getNumBlocks() * DataNode.BLOCK_SIZE)
            throw new IllegalArgumentException();
        channels.remove(fileUuid);
    }

    /**
     * 递归创建目录
     * 如果父目录不存在，则先创建父目录
     *
     * @param fileUri the directory path
     */
    @Override
    public void mkdir(String fileUri) throws InvalidPathException, FileAlreadyExistsException {
        if (FileUtil.isValidDir(fileUri)) {
            if (rootNode.lookUp(fileUri, Node.TYPE.DIR) != null)
                throw new FileAlreadyExistsException(fileUri);
            String dirName = FileUtil.getName(fileUri);
            String parentPath = fileUri.substring(0, fileUri.lastIndexOf("/" + dirName) + 1);
            DirNode parentDNode = (DirNode) rootNode.lookUp(parentPath, Node.TYPE.DIR);
            if (parentDNode == null) {
                mkdir(parentPath);
                parentDNode = (DirNode) rootNode.lookUp(parentPath, Node.TYPE.DIR);
            }
            DirNode dirNode = new DirNode();
            addNode(dirNode);
            parentDNode.addEntry(dirName, dirNode);
            addNode(parentDNode);
        }
    }

    /**
     * 为一个文件添加一个空闲块，这个块备份blockAmount次，即每个文件的块有blockAmount个备份
     *
     * @param fileUuid    the file uuid with readwrite state
     * @param blockAmount the request block amount
     * @return 返回空闲块列表
     */
    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        if (fileUuid == null || !channels.containsKey(fileUuid))
            throw new IllegalStateException();
        SDFSFileChannel channel = getReadwriteFile(fileUuid);
        if (blockAmount < 0)
            return null;
        FileNode node = channel.getFileNode();
        ArrayList<LocatedBlock> blocks = new ArrayList<>();
        for (int i = 0; i < blockAmount; i++) {
            blocks.add(new LocatedBlock(blockId++));
        }
        BlockInfo info = new BlockInfo();
        info.addLocatedBlocks(blocks);
        node.addBlockInfo(info);
        return blocks;
    }

    /**
     * 删掉一个文件的最后blockAmount个块
     *
     * @param fileUuid    the file uuid with readwrite state
     * @param blockAmount the blocks amount to be removed
     * @throws IllegalStateException
     * @throws IllegalArgumentException
     */
    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException, IllegalArgumentException {
        if (fileUuid == null || !channels.containsKey(fileUuid))
            throw new IllegalStateException();
        SDFSFileChannel rwc = getReadwriteFile(fileUuid);
        if (blockAmount < 0 || rwc.getNumBlocks() < blockAmount)
            throw new IllegalArgumentException();
        rwc.setFileSize((rwc.getNumBlocks() - blockAmount) * DataNode.BLOCK_SIZE);
    }

    /**
     * 将文件元数据信息node写入磁盘
     */
    private void addNode(Node node) {
        try {
            ObjectOutputStream os =
                    new ObjectOutputStream(new FileOutputStream(new File(NAME_NODE_DATA_DIR + node.getNodeId() + ".node")));
            os.writeObject(node);
            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断一个文件fileNode是否以“读写”方式打开两次
     */
    private boolean readWriteTwice(FileNode fileNode) {
        Iterator<SDFSFileChannel> it;
        SDFSFileChannel channel;
        for (it = channels.values().iterator(); it.hasNext(); ) {
            channel = it.next();
            if (!channel.isReadOnly() && channel.getFileNodeId() == fileNode.getNodeId() && channel.isOpen())
                throw new IllegalStateException();
        }
        return false;
    }
}
