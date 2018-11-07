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
        if (host == null)
            host = "";
        if (port < 0) {
            port = 0;
        }
        if (service == null) {
            service = "";
        }
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
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || obj.getClass() != getClass())
            return false;
        Url that = (Url) obj;
        return host.equals(that.host) && port == that.port && service.equals(that.service);
    }

    @Override
    public String toString() {
        return host + ":" + port + "/" + service;
    }
}
