/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.AbstractStub;
import sdfs.Invocation;
import sdfs.Url;
import sdfs.datanode.DataNode;
import sdfs.datanode.IDataNode;

import java.util.UUID;

import static sdfs.Contants.DEFAULT_DATA_NODE_PORT;

public class DataNodeStub extends AbstractStub implements IDataNode {
    private Url url;

    public DataNodeStub() {
        url = new Url("", DEFAULT_DATA_NODE_PORT, DataNode.class.getName());
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException {
        Invocation invocation = new Invocation("read",
                new Class[]{UUID.class, int.class, int.class, int.class},
                new Object[]{fileUuid, blockNumber, offset, size});
        Object obj = invoke(url, invocation);
        return (byte[]) obj;
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException {
        Invocation invocation = new Invocation("write",
                new Class[]{UUID.class, int.class, int.class, byte.class},
                new Object[]{fileUuid, blockNumber, offset, b});
        invoke(url, invocation);
    }
}
