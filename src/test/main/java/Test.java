import cn.xnatural.aio.AioClient;
import cn.xnatural.aio.AioServer;
import cn.xnatural.aio.AioStream;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {

    public static void main(String[] args) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(2);
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("delimiter", "\n");
        AioServer server = new AioServer(attrs, exec) {
            @Override
            protected void receive(byte[] bs, AioStream stream) {
                log.info("服务端 -> 接收数据: " + new String(bs));
                try {
                    Thread.sleep(1000 * 4);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                stream.write(ByteBuffer.wrap("回消息: xxxxxxxxxxxxxx\n".getBytes()), (e, stream1) -> {
                    log.info("回消息 失败", e);
                }, () -> {
                    log.info("回消息 成功");
                });
            }
        };
        server.start();

        AioClient client = new AioClient(attrs, exec) {
            @Override
            protected void receive(byte[] bs, AioStream stream) {
                log.info("客户端 -> 接收数据: " + new String(bs));
                try {
                    Thread.sleep(1000 * 2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                stream.write(ByteBuffer.wrap("回消息: xxxxxxxxxxxxxx\n".getBytes()));
            }
        };
        client.send("localhost", 7001, "Hello\n".getBytes());

        Thread.sleep(1000 * 60 * 1);
        server.stop();
        client.stop();
        exec.shutdown();
    }
}
