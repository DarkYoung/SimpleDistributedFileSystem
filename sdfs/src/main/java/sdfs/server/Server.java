package sdfs.server;

public interface Server {
    void register(String host, int port, Class service);

    void unRegister();

    void listenRequest(int port);
}
