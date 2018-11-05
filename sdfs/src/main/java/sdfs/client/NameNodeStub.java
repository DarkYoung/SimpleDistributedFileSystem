/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.AbstractStub;
import sdfs.Constants;
import sdfs.Invocation;
import sdfs.Url;
import sdfs.filetree.LocatedBlock;
import sdfs.namenode.INameNode;
import sdfs.namenode.NameNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.InvalidPathException;
import java.util.List;
import java.util.UUID;


public class NameNodeStub extends AbstractStub implements INameNode {
    private Url url;

    public NameNodeStub() {
        url = new Url("", Constants.DEFAULT_NAME_NODE_PORT, NameNode.class.getName());
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws FileNotFoundException, InvalidPathException {
        Invocation invocation = new Invocation("openReadonly",
                new Class[]{String.class}, new Object[]{fileUri});
        return (SDFSFileChannel) invoke(url, invocation);
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException {
        Invocation invocation = new Invocation("openReadwrite",
                new Class[]{String.class}, new Object[]{fileUri});
        return (SDFSFileChannel) invoke(url, invocation);
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IllegalStateException {
        Invocation invocation = new Invocation("create",
                new Class[]{String.class}, new Object[]{fileUri});
        return (SDFSFileChannel) invoke(url, invocation);
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException {
        Invocation invocation = new Invocation("closeReadonlyFile",
                new Class[]{UUID.class}, new Object[]{fileUuid});
        invoke(url, invocation);
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        Invocation invocation = new Invocation("closeReadwriteFile",
                new Class[]{UUID.class, int.class}, new Object[]{fileUuid, newFileSize});
        invoke(url, invocation);
    }

    @Override
    public void mkdir(String fileUri) throws InvalidPathException, FileAlreadyExistsException {
        Invocation invocation = new Invocation("mkdir",
                new Class[]{String.class}, new Object[]{fileUri});
        invoke(url, invocation);
    }


    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        Invocation invocation = new Invocation("addBlocks",
                new Class[]{UUID.class, int.class}, new Object[]{fileUuid, blockAmount});
        return (List<LocatedBlock>) invoke(url, invocation);

    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        Invocation invocation = new Invocation("removeLastBlocks",
                new Class[]{UUID.class, int.class}, new Object[]{fileUuid, blockAmount});
        invoke(url, invocation);
    }

    public SDFSFileChannel getReadonlyFile(UUID fileUuid) throws IllegalStateException {
        Invocation invocation = new Invocation("getReadonlyFile",
                new Class[]{UUID.class}, new Object[]{fileUuid});
        return (SDFSFileChannel) invoke(url, invocation);
    }

    public SDFSFileChannel getReadwriteFile(UUID fileUuid) throws IllegalStateException {
        Invocation invocation = new Invocation("getReadwriteFile",
                new Class[]{UUID.class}, new Object[]{fileUuid});
        return (SDFSFileChannel) invoke(url, invocation);
    }
}
