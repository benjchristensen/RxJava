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
                .parallel(new Func1<Observable<TcpConnection>, Observable<ReceivedMessage>>() {

                    @Override
                    public Observable<ReceivedMessage> call(Observable<TcpConnection> o) {
                        // for each connection 
                        return o.flatMap(new Func1<TcpConnection, Observable<ReceivedMessage>>() {

                            @Override
                            public Observable<ReceivedMessage> call(final TcpConnection connection) {
                                // for each message we receive on the connection
                                return connection.getChannelObservable().map(new Func1<ByteBuf, ReceivedMessage>() {

                                    @Override
                                    public ReceivedMessage call(ByteBuf o) {
                                        String msg = o.toString(Charset.forName("UTF8")).trim();

                                        return new ReceivedMessage(connection, msg);
                                    }

                                });
                            }

                        });

                    }
                })
                .toBlockingObservable().forEach(new Action1<ReceivedMessage>() {

                    @Override
                    public void call(ReceivedMessage receivedMessage) {
                        receivedMessage.connection.write("Echo => " + receivedMessage.message + "\n");
                        System.out.println("Received Message: " + receivedMessage.message);
                    }
                });
    }

    public static class ReceivedMessage {
        // I want tuples in java

        final TcpConnection connection;
        final String message;

        public ReceivedMessage(TcpConnection connection, String message) {
            this.connection = connection;
            this.message = message;
        }
    }
}
