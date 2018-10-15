package sdfs.datanode;


import java.io.*;
import java.util.UUID;

public class DataNode implements IDataNode, Serializable {

    private static final long serialVersionUID = 963270564695516201L;

    private DataNode() {
        // TODO, your code here
    }

    public static DataNode getInstance() {
        return SingletonHolder.INSTANCE;
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size)
            throws IllegalStateException, IndexOutOfBoundsException, IOException {
        // TODO your code here
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

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b)
            throws IllegalStateException, IndexOutOfBoundsException, IOException {
        // TODO your code here
        File file = new File(DATANODE_DATA_DIR + blockNumber + ".block");
        if (offset < 0 || offset >= BLOCK_SIZE || offset + b.length > BLOCK_SIZE)
            throw new IndexOutOfBoundsException();
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
        outputStream.write(b, offset, b.length);
    }


    /**
     * In the first lab, DataNode is a singleton
     */
    private static class SingletonHolder {
        private static final DataNode INSTANCE = new DataNode();
    }
}
