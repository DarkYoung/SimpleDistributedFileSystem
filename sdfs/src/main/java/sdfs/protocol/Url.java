package sdfs.protocol;

import sdfs.Constants;

public class Url {
    private final String host;
    private final String service;
    private volatile int port;

    public Url(String host, String service) {
        this(host, Constants.DEFAULT_PORT, service);
    }

    public Url(String host, int port, String service) {
        this.host = host;
        this.port = port;
        this.service = service;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getService() {
        return service;
    }

    @Override
    public String toString() {
        return host + ":" + port + "/" + service;
    }
}
