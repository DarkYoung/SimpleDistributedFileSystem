﻿# Document 

**version: 1.1.0**

————16302010059 张健

标签（空格分隔）： CSE SDFS Java RPC

---

# 1. **Architecture:**
> **整个系统包含三个主要角色：`NameNode`、`DataNode`、`Client`** 

***角色交互过程：***

- [x] **创建文件：**
* Client->create
* NameNodeStub->create
* NameNode->onRpcReceive
* NameNode->create return	SDFSFileChannel	with readwrite permission	
* NameNodeStub receive and return SDFSFileChannel

---
- [x] **写入数据：**
* SDFSFileChannel->write block not enough	
* NameNodeStub->addBlock	
* NameNodeServer->onRpcReceive	
* NameNode->addBlock return	LocatedBlock	
* NameNodeStub receive and return LocatedBlock	
* SDFSFileChannel continue to write	data
* SDFSFileChannel->close	
* SDFSFileChannel->flush	
* DataNodeStub->write	
* DataNode->onRpcReceive	
* DataNode->write to disk and return result	
* DataNodeStub receive and return result	
* SDFSFileChannel close	successful
---

## 1.1. **client:**
> **客户端**
### 1.1.1. **Stub:**
> **`RPC接口`** 
> **在提供强大的远程调用能力时不损失本地调用的语义简洁性**
```java
    Object invoke(Url url, Invocation invocation) throws Exception;
```
#### 1.1.1.1. AbstractStub:
> **`抽象类`**
> **实现Stub接口**
> * 连接服务器
> * 发送请求
> * 接收请求

##### DataNodeStub:
> **继承AbstractStub**

##### NameNodeStub:
> **继承AbstractStub**

___ 

### 1.1.2. **SDFSFileChannel:**
> **文件输入/输出流，含有读写指针，数据同步**
___ 

### 1.1.3. **SDFSClient:**
> **客户端，通过Stub进行远程调用**

--- 

## 1.2. **server:**
> **服务器端**

### 1.2.1. **DataNode:**
> **作为SDFS系统中存储数据的终端，提供`read`和`write`方法来读写对应blockNumber标识的数据块，在读写的过程中可能会涉及到偏移量等参数**
___

### 1.2.2. **NameNode:**
> **维护整个SDFS的INode信息，负责响应Client发起的操作，同时负责分配空闲块以及释放非空闲块**
___

### 1.2.3. **FileTree:**
> **文件树结构**
 
#### 1.2.3.1. **DirNode:**
> **目录节点，是文件树结构中的中间/叶子节点，可能有子节点**

##### 1.2.3.2. **Entry:**
> **以name-->node的键值对形式保存文件（文件夹/文件）节点信息**

#### 1.2.3.3. **FileNode:**
> **文件节点，是文件树结构中的叶子节点**

##### **BlockInfo:**
> **文件的一个数据块信息，每个文件包含一个List<BlockInfo>属性，即所有的数据块**

##### **LocatedBlock:**
> **文件的一个数据块的数据备份（包含原始数据块），每个BlockInfo（即每个数据块）都有几个备份，即一个List<LocatedBlock>**
> **在分布式文件系统中，这种备份形式使得文件的容错性更高，当一个数据块失效时，可以通过数据块的备份恢复这个块**

___

### **Server:**
> **`服务器接口`**
```java
    void register(String host, int port, Class service);

    void unRegister();

    void listenRequest(int port);
```
#### **AbstractServer:**
> **`抽象类`**
> **实现Server接口**
> * 注册服务，如将服务（如NameNode）注册：将NameNode所在ip、所监听的端口号以及所提供的服务注册到`服务注册中心`
> * 取消注册
> * 监听端口，并响应请求

---

## 1.3. **protocol:**
> **`通信协议`**

### 1.3.1 **Invocation:**
> **`请求体`**
> **定义了客户端远程调用的请求的数据结构**
> * 函数名
> * 参数类型
> * 参数值

___
### 1.3.2 **Response:**
> **`响应体`**
> **定义了服务器端响应请求的数据结构**
> * 返回值类型
> * 返回值
> * 异常：远程调用异常或者函数（远程调用的函数）异常
---

## 1.4. **util:**
### 1.4.1. **FileUtil:**
**关于文件的工具类，判断路径是否合法有效**

---

## 1.5. **Constants:**
> **`常量类`**
> **定义了常用的端口号、ip/host等**

---

## 1.6. **Registry:**
> **`服务注册中心`** 
> **为服务器（NameNode、DataNode）提供注册服务，为客户端提供订阅服务** 
> 提供负载均衡策略选择实例 
>> 随机选择 
>> 匹配选择 
```java
/**
 * 负载均衡策略
 */
interface Strategy {
    Url getUrl(Url src, List<Url> urls);

}

/**
 * 随机选择
 */
class RandomStrategy implements Strategy {
    @Override
    public Url getUrl(Url src, List<Url> urls) {
        int size = urls.size();
        int index = 0;
        if (size > 0)
            index = (int) ((Math.random() * 1000) % size);
        return urls.get(index);
    }
}

/**
 * 匹配选择
 */

class MatchStrategy implements Strategy {

    @Override
    public Url getUrl(Url src, List<Url> urls) {
        for (Url dst : urls) {
            if (dst.equals(src)) {
                return dst;
            }
        }
        return null;
    }
}
```

---

# 2. **RPC实现:**
> **目标：在提供强大的远程调用能力时不损失本地调用的语义简洁性** 
> **类别：同步调用，客户端等待调用执行完成并返回结果** 
> **分布式实现：服务注册中心** 
>> 注册多服务（多个NameNode，多个DataNode） 
>> 使用负载均衡策略选择实例（随机策略、匹配策略） 
> **异常处理** 
>> 本地调用一定会执行，而远程调用则不一定，如服务器未接收到请求 
>> 本地调用只会抛出接口声明异常，而远程调用还会抛出RPC运行时异常 

## 2.1. **具体实现：**
### 2.1.1. **客户端**
```java
    //调用过程
    @Override
    public Object invoke(Url url, Invocation invocation) throws Exception {
        invocation.setStub(this);
        return doInvoke(url, invocation);
    }

    private Object doInvoke(Url url, Invocation invocation) throws Exception {
        Url serverUrl = Registry.chooseTarget(url);
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
                if (response.getException() != null) { //抛出异常
                    throw response.getException();
                }
                return response.getReturnValue();
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
```
___

### 2.1.2 **服务器端：**
```java
    @Override
    public void listenRequest(int port) {
        try (ServerSocket listener = new ServerSocket(port)) {
            while (true) {
                try (Socket socket = listener.accept()) { //接收请求
                        try {
                            Response response = new Response();
                            //将请求反序列化
                            ObjectInputStream objectInputStream = null;
                            objectInputStream = new ObjectInputStream(socket.getInputStream());
                            Object object = null;
                            try {
                                object = objectInputStream.readObject();
                                //调用服务
                                if (object instanceof Invocation) {
                                    //利用反射机制调用对应的方法
                                    Invocation invocation = (Invocation) object;
                                    Method method = getClass().getMethod(invocation.getMethodName(), invocation.getParameterTypes());
                                    response.setReturnType(method.getReturnType());
                                    response.setReturnValue(method.invoke(this, invocation.getArguments()));
                                } else {
                                    throw new UnsupportedOperationException();
                                }
                            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                                response.setException(e); //返回异常
                            } finally {
                                //返回结果
                                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                                objectOutputStream.writeObject(response);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            unRegister(); //关闭服务器后，将注册的服务移除
        }
    }
```

---

# 3. **测试（修改）：**
> **对部分测试的修改说明**

## 3.1. **NameNodeTest & DataNodeTest:**
> **调用register方法，将服务器注册到服务注册中心，供客户端选择**
> **启动一个线程运行服务器**
```java
    nameNode.register(DEFAULT_HOST, DEFAULT_NAME_NODE_PORT);
    new Thread(nameNode::listenRequest).start();
```
```java
    new Thread(dataNode::listenRequest);
```
## 3.2. **SDFSStubTest:**
> **调用register方法，将服务器注册到服务注册中心，供客户端选择**
> **测试多个DataNode同时运行的情况**
```java
    for (int i = 1; i <= 5; i++) {//模拟生成5个DataNode服务器（同一台主机，监听不同的端口）
        DataNode dataNode = new DataNode();
        dataNode.register("localhost", Constants.DEFAULT_DATA_NODE_PORT + i);
        new Thread(dataNode::listenRequest).start();
    }
```

## 3.3. **提示：**
> NameNodeTest不可以运行测试类，即不可以同时测所有方法

**原因：**
> 每个Test方法都会注册一个NameNode服务
> 根据分布式文件系统要求，如果有多个NameNode，那么它们间应该是数据互通的
> 分析NameNodeTest可知，整个测试类的5个NameNode不应该数据互通
> 因此直接运行测试类会导致，如Test2连接了Test1注册的NameNode服务
> 然而本次测试的NameNode间数据不共享，因此只能依次运行Test方法

---

# 4. **Bonus:**
- [x] 数据备份
> * LocatedBlock类新增host、port属性
> * 调用addBlocks方法时，为每个块分配多个备份块
> * 为每个备份随机选择一个可用的DataNode服务器，即设置host为某个DataNode的ip、设置port为对应DataNode监听的端口
> * 当读取的数据块失效时，选择读取可用的备份块
**部分实现代码：**
```java
        ArrayList<LocatedBlock> blocks = new ArrayList<>();
        for (int i = 0; i < blockAmount; i++) {
            Url url = Registry.chooseTarget(dataNodeUrl);   //随机选择一个DataNode服务器
            blocks.add(new LocatedBlock(url.getHost(), url.getPort(), blockId++));
        }
```
```java
           while (blockIt.hasNext()) { //每个块有多个备份，如果一个块失效了，那么使用另一个（对应下面的continue）
```
---

# 5. **Difficulties:**
- [x] 使用缓冲区解决数据同步问题
---




