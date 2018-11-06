package sdfs.client;


import sdfs.protocol.Invocation;
import sdfs.protocol.Url;

public interface Stub {
    Object invoke(Url url, Invocation invocation);
}
