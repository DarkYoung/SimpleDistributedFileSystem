package sdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public abstract class AbstractStub implements Stub {
    //调用过程
    @Override
    public Object invoke(Url url, Invocation invocation) {
        invocation.setStub(this);
        return doInvoke(url, invocation);
    }

    private Object doInvoke(Url url, Invocation invocation) {
        Url serverUrl = Registry.chooseTarget(Registry.lookupUrls(url));
        //连接服务器
        try {
            Socket socket = new Socket(serverUrl.getHost(), serverUrl.getPort());
            //将请求序列化
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

            //将请求发给服务器提供方
            objectOutputStream.writeObject(invocation);

            //将响应体反序列化
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            Object object = objectInputStream.readObject();
            if (object instanceof Response) {
                Response response = (Response) object;
                if (response.getException() != null) {
                    response.getException().printStackTrace();
                    return null;
                }
                return response.getReturnValue();
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
