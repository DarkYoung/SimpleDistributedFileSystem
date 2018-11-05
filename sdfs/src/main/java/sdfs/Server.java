package sdfs;

public interface Server {
    void register(String host, int port, Class service);

    void listenRequest(int port);
}
