package sdfs.datanode;


import sdfs.Registry;
import sdfs.Url;

import java.io.*;
import java.util.UUID;

import static sdfs.Contants.DEFAULT_DATA_NODE_PORT;

public class DataNode implements IDataNode, Serializable {

    private static final long serialVersionUID = 963270564695516201L;

    private int port = DEFAULT_DATA_NODE_PORT;

    public DataNode() {
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
        //TODO
    }

    /**
     * 从指定block（blockNumber.block）处读取数据
     *
     * @param fileUuid    the file uuid to check whether have permission to read or not. (not used in lab 1)
     * @param blockNumber the block number to be read
     * @param offset      the offset on the block file
     * @param size        the total size to be read
     * @return 读取的字节数组
     */
    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size)
            throws IllegalStateException, IndexOutOfBoundsException, IOException {
        File file = new File(DATANODE_DATA_DIR + blockNumber + ".block");
        if (!file.isFile()) {
            throw new FileNotFoundException();
        }
        if (offset < 0 || offset >= BLOCK_SIZE || offset + size > BLOCK_SIZE)
            throw new IndexOutOfBoundsException();
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));
        byte[] bytes = new byte[size];
        inputStream.read(bytes, offset, size);
        return bytes;
    }

    /**
     * 写入数据到指定block（blockNumber.block)
     *
     * @param fileUuid    the file uuid to check whether have permission to write or not. (not used in lab 1)
     * @param blockNumber the block number to be written
     * @param offset      the offset on the block file
     * @param b           the buffer that stores the data
     */
    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b)
            throws IllegalStateException, IndexOutOfBoundsException, IOException {
        File file = new File(DATANODE_DATA_DIR + blockNumber + ".block");
        if (offset < 0 || offset >= BLOCK_SIZE || offset + b.length > BLOCK_SIZE)
            throw new IndexOutOfBoundsException();
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
        outputStream.write(b, offset, b.length);
    }
}
