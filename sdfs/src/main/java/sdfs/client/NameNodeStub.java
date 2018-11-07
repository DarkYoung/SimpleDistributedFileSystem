/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.Constants;
import sdfs.protocol.Invocation;
import sdfs.protocol.Url;
import sdfs.server.filetree.LocatedBlock;
import sdfs.server.namenode.INameNode;
import sdfs.server.namenode.NameNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;
import java.util.List;
import java.util.UUID;


public class NameNodeStub extends AbstractStub implements INameNode {
    private Url url;

    public NameNodeStub() {
        //这里直接调用DataNode的类类型来获取包名
        //实际调用时应该直接写包名（如：sdfs.server.namenode.NameNode）
        url = new Url("", Constants.DEFAULT_NAME_NODE_PORT, NameNode.class.getName());
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws FileNotFoundException, InvalidPathException {
        Invocation invocation = new Invocation("openReadonly",
                new Class[]{String.class}, new Object[]{fileUri});
        SDFSFileChannel channel = null;
        try {
            channel = (SDFSFileChannel) invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof FileNotFoundException)
                throw new FileNotFoundException();
            else if (e instanceof InvalidPathException)
                throw new InvalidPathException(((InvalidPathException) e).getInput(), ((InvalidPathException) e).getReason());
            else e.printStackTrace();
        }
        return channel;
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException {
        Invocation invocation = new Invocation("openReadwrite",
                new Class[]{String.class}, new Object[]{fileUri});
        SDFSFileChannel channel = null;
        try {

            channel = (SDFSFileChannel) invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof IndexOutOfBoundsException)
                throw new IndexOutOfBoundsException();
            else if (e instanceof IllegalStateException)
                throw new IllegalStateException();
            else e.printStackTrace();
        }
        return channel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IllegalStateException {
        Invocation invocation = new Invocation("create",
                new Class[]{String.class}, new Object[]{fileUri});
        SDFSFileChannel channel = null;
        try {

            channel = (SDFSFileChannel) invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof IllegalStateException)
                throw new IllegalStateException();
            else e.printStackTrace();
        }
        return channel;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException {
        Invocation invocation = new Invocation("closeReadonlyFile",
                new Class[]{UUID.class}, new Object[]{fileUuid});
        try {
            invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof IllegalStateException) {
                throw new IllegalStateException();
            } else e.printStackTrace();
        }
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        Invocation invocation = new Invocation("closeReadwriteFile",
                new Class[]{UUID.class, int.class}, new Object[]{fileUuid, newFileSize});
        try {
            invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof IllegalStateException) {
                throw new IllegalStateException();
            } else if (e instanceof IllegalArgumentException) {
                throw new IllegalArgumentException();
            } else if (e instanceof IOException) {
                throw new IOException();
            } else e.printStackTrace();
        }
    }

    @Override
    public void mkdir(String fileUri) throws InvalidPathException, FileAlreadyExistsException {
        Invocation invocation = new Invocation("mkdir",
                new Class[]{String.class}, new Object[]{fileUri});
        try {
            invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof InvalidPathException) {
                throw new InvalidPathException((((InvalidPathException) e).getInput()), ((InvalidPathException) e).getReason());
            } else if (e instanceof FileAlreadyExistsException) {
                throw new FileAlreadyExistsException(((FileAlreadyExistsException) e).getFile());
            } else e.printStackTrace();
        }
    }


    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        Invocation invocation = new Invocation("addBlocks",
                new Class[]{UUID.class, int.class}, new Object[]{fileUuid, blockAmount});
        List<LocatedBlock> blocks = null;
        try {
            blocks = (List<LocatedBlock>) invoke(url, invocation);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return blocks;
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        Invocation invocation = new Invocation("removeLastBlocks",
                new Class[]{UUID.class, int.class}, new Object[]{fileUuid, blockAmount});
        try {
            invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof IllegalStateException) {
                throw new IllegalStateException();
            }
        }
    }

    public SDFSFileChannel getReadonlyFile(UUID fileUuid) throws IllegalStateException {
        Invocation invocation = new Invocation("getReadonlyFile",
                new Class[]{UUID.class}, new Object[]{fileUuid});
        SDFSFileChannel channel = null;

        try {
            channel = (SDFSFileChannel) invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof IllegalStateException)
                throw new IllegalStateException();
        }
        return channel;
    }

    public SDFSFileChannel getReadwriteFile(UUID fileUuid) throws IllegalStateException {
        Invocation invocation = new Invocation("getReadwriteFile",
                new Class[]{UUID.class}, new Object[]{fileUuid});
        SDFSFileChannel channel = null;
        try {
            channel = (SDFSFileChannel) invoke(url, invocation);
        } catch (Exception e) {
            if (e instanceof IllegalStateException)
                throw new IllegalStateException();
        }

        return channel;
    }
}
