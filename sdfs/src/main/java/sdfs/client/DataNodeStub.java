/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.Constants;
import sdfs.protocol.Invocation;
import sdfs.protocol.Url;
import sdfs.server.datanode.DataNode;
import sdfs.server.datanode.IDataNode;

import java.util.UUID;


public class DataNodeStub extends AbstractStub implements IDataNode {
    private Url url;

    public DataNodeStub() {
        //这里直接调用DataNode的类类型来获取包名
        //实际调用时应该直接写包名（如：sdfs.server.datanode.DataNode）
        url = new Url("", Constants.DEFAULT_DATA_NODE_PORT, DataNode.class.getName());
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
                new Class[]{UUID.class, int.class, int.class, byte[].class},
                new Object[]{fileUuid, blockNumber, offset, b});
        invoke(url, invocation);
    }
}
