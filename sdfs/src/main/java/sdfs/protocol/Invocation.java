package sdfs.protocol;

import sdfs.client.Stub;

import java.io.Serializable;

/**
 * 请求数据结构协议：
 * 方法名
 * 参数类型
 * 参数值
 */
public class Invocation implements Serializable {
    private static final long serialVersionUID = -1908860219241192698L;
    private String methodName;
    private Class<?>[] parameterTypes;
    private Object[] arguments;
    private transient Stub stub;

    public Invocation(String methodName, Class<?>[] parameterTypes, Object[] arguments) {
        this.methodName = methodName;
        this.parameterTypes = parameterTypes;
        this.arguments = arguments;
    }

    public void setStub(Stub stub) {
        this.stub = stub;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public Stub getStub() {
        return stub;
    }
}
