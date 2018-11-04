package sdfs;

import java.io.Serializable;

/**
 * 响应数据结构：
 * 返回值类型
 * 返回值
 * 异常
 * 异常信息
 */
public class Response implements Serializable{
    private static final long serialVersionUID = -6821700354840913707L;
    private Class returnType;
    private Object returnValue;
    private Exception exception;

    public Response() {
        this(null, null, null);
    }

    public Response(Class returnType, Object returnValue, Exception e) {
        this.returnType = returnType;
        this.returnValue = returnValue;
        this.exception = e;
    }

    public void setReturnType(Class returnType) {
        this.returnType = returnType;
    }

    public void setReturnValue(Object returnValue) {
        this.returnValue = returnValue;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Class getReturnType() {
        return returnType;
    }

    public Object getReturnValue() {
        return returnValue;
    }

    public Exception getException() {
        return exception;
    }
}
