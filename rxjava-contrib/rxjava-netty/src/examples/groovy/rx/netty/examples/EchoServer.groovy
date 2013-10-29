package rx.netty.examples;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

import rx.Observable;
import rx.netty.experimental.RxNetty;
import rx.netty.experimental.impl.TcpConnection;
import rx.util.functions.Action1;
import rx.util.functions.Func1;

public class EchoServer {

    public static void main(String[] args) {
        RxNetty.createTcpServer(8181)
                // process each connection in parallel
                .parallel({ Observable<TcpConnection<String, String>> o ->
                    // for each connection
                    return o.flatMap({ TcpConnection<String, String> connection ->
                        // for each message we receive on the connection
                        return connection.getChannelObservable().map({ String msg ->
                            return new ReceivedMessage<String>(connection, msg.trim());
                        });
                    });
                })
                .toBlockingObservable().forEach({ ReceivedMessage<String> receivedMessage ->
                    receivedMessage.connection.write("Echo => " + receivedMessage.message + "\n");
                    System.out.println("Received Message: " + receivedMessage.message);
                });
    }

    def static class ReceivedMessage<I> {
        // I want value types

        final TcpConnection<I, String> connection;
        final String message;

        public ReceivedMessage(TcpConnection<I, String> connection, String message) {
            this.connection = connection;
            this.message = message;
        }
    }
}
