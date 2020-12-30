#### 介绍
轻量级 tcp 数据接收发送工具. 基于 jdk aio AsynchronousSocketChannel

适用协议: gt06, jt808, rpc 等


#### 安装教程
```
<dependency>
    <groupId>cn.xnatural.aio</groupId>
    <artifactId>aio</artifactId>
    <version>1.0.7</version>
</dependency>
```

#### 组件介绍
* AioClient: 客户端. 发送数据,接收回应
    
```
AioClient client = new AioClient(attrs, exec) { // 创建客户端
    @Override
    protected void receive(byte[] bs, AioStream stream) {
        log.info("客户端 -> 接收数据: " + new String(bs) + ", length: " + bs.length);
        stream.reply(("客户端 -> 回消息: xx").getBytes());
    }
};
// 向本机 7001 发送 Hello 消息
client.send("localhost", 7001, "Hello".getBytes("utf-8"))
```

* AioServer: 服务端. 接收连接,包装成AioStream, 处理stream中过来的消息
```
ExecutorService exec = Executors.newFixedThreadPool(2);
Map<String, Object> attrs = new HashMap<>(); // 属性集
attrs.put("delimiter", "\n"); //分隔符 用于tcp拆粘包
attrs.put("hp", ":7001"); //hp -> [ip]:端口. 监听7001端口

AioServer server = new AioServer(attrs, exec) { // 创建服务端
    @Override
    protected void receive(byte[] bs, AioStream stream) { //数据接收
        log.info("服务端 -> 接收数据: " + new String(bs) + ", length: " + bs.length);
        stream.reply(
                ("服务端 回消息: oo").getBytes(), //消息体字节
                (ex, me) -> { //失败回调函数
                    log.error("服务端 -> 回消息 失败", ex);
                }, () -> { //成功回调函数
                    log.info("服务端 -> 回消息 成功");
                });
    }
};
server.start(); //开始监听
```

* AioStream: Aio(AsynchronousSocketChannel)连接封装, 写数据, 读数据
```
/**
 * 回应消息
 * @param bs 消息字节数组
 * @param failFn 回应失败回调函数
 * @param okFn 回应成功回调函数
 */
reply(byte[] bs, BiConsumer<Exception, AioStream> failFn, Runnable okFn)
```



#### 参与贡献

xnatural@msn.cn
