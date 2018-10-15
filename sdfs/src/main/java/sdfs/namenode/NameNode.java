package sdfs.namenode;

import sdfs.client.SDFSFileChannel;
import sdfs.filetree.*;
import sdfs.util.FileUtil;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;
import java.util.*;

public class NameNode implements INameNode {
    private Map<UUID, SDFSFileChannel> channels;
    private DirNode rootNode;
    private static String rootNodePath = NAMENODE_DATA_DIR + "0.node";
    private static int blockId = 0;

    private NameNode() {
        // TODO, your code here
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
    }

    public static NameNode getInstance() {
        return SingletonHolder.INSTANCE;
    }


    /**
     * 返回指定路径的SDFSFileChannel对象
     * SDFSFileChannel包括对应FileNode信息
     * 每个FileNode包括多个BlockInfo
     * 根据BlockInfo对象可以寻找到单个块对应的所有备份，即LocatedBlock对象列表
     * 根据不同的LocatedBlock中的blockNumber寻找到对应的DataNode节点上的数据块
     *
     * @param fileUri The file uri to be open
     * @return SDFSFileChannel Instance
     * @throws InvalidPathException
     * @throws FileNotFoundException
     */
    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws InvalidPathException, FileNotFoundException {
        // TODO your code here
        if (fileUri == null || fileUri.isEmpty())
            throw new InvalidPathException("", "");
        FileNode fileNode = (FileNode) rootNode.lookUp(fileUri, Node.TYPE.FILE);
        if (fileNode != null) {
            SDFSFileChannel channel = new SDFSFileChannel(UUID.randomUUID(), fileNode, true);
            channels.put(channel.getUuid(), channel);
            return channel;
        }
        return null;
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws InvalidPathException, FileNotFoundException, IllegalStateException {
        // TODO your code here
        FileNode fileNode = (FileNode) rootNode.lookUp(fileUri, Node.TYPE.FILE);
        if (fileNode != null && !readWriteTwice(fileNode)) {
            SDFSFileChannel channel = new SDFSFileChannel(UUID.randomUUID(), fileNode, false);
            channels.put(channel.getUuid(), channel);
            return channel;
        }
        return null;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws FileAlreadyExistsException, InvalidPathException {
        // TODO your code here
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

    @Override
    public SDFSFileChannel getReadonlyFile(UUID fileUuid) throws IllegalStateException {
        // TODO your code here
        SDFSFileChannel channel = channels.get(fileUuid);
        if (!channel.isReadOnly())
            throw new IllegalStateException();
        return channel;

    }

    @Override
    public SDFSFileChannel getReadwriteFile(UUID fileUuid) throws IllegalStateException {
        // TODO your code here
        SDFSFileChannel channel = channels.get(fileUuid);
        if (channel.isReadOnly())
            throw new IllegalStateException();
        return channel;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException {
        // TODO your code here
//        SDFSFileChannel channel = channels.get(fileUuid);
//        channel.close();
        channels.remove(fileUuid);
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException {
        // TODO your code here
//        SDFSFileChannel channel = channels.get(fileUuid);
//        channel.close();
        channels.remove(fileUuid);
    }

    @Override
    public void mkdir(String fileUri) throws InvalidPathException, FileAlreadyExistsException {
        // TODO your code here
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

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        // TODO your code here
        if (fileUuid == null || blockAmount < 0)
            return null;
        SDFSFileChannel channel = getReadwriteFile(fileUuid);
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

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException, IllegalArgumentException {
        // TODO your code here
        SDFSFileChannel rwc = getReadwriteFile(fileUuid);
        if (blockAmount < 0 || rwc.getNumBlocks() < blockAmount)
            throw new IllegalArgumentException();
        try {
            FileNode node = rwc.getFileNode();
            ObjectOutputStream outputStream =
                    new ObjectOutputStream(new FileOutputStream(new File(NAMENODE_DATA_DIR + node.getNodeId() + ".node")));
            node.removeLastBlocks(blockAmount);
            outputStream.writeObject(node);
        } catch (IOException ignored) {

        }
    }

    private void addNode(Node node) {
        try {
            ObjectOutputStream os =
                    new ObjectOutputStream(new FileOutputStream(new File(NAMENODE_DATA_DIR + node.getNodeId() + ".node")));
            os.writeObject(node);
            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

    /**
     * In the first lab, NameNode is a singleton
     */
    private static class SingletonHolder {
        private static final NameNode INSTANCE = new NameNode();
    }
}
