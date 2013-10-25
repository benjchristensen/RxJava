package rx.netty.examples;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

import rx.Observable;
import rx.netty.experimental.RxNetty;
import rx.netty.experimental.impl.TcpConnection;
import rx.util.functions.Action1;
import rx.util.functions.Func1;

public class IntervalClientWithDisconnect {

    public static void main(String[] args) {
        new IntervalClientWithDisconnect().run();
    }

    public void run() {
        RxNetty.createTcpClient("localhost", 8181)
                .flatMap({ TcpConnection connection ->
                    System.out.println("received connection: " + connection);

                    Observable<String> subscribeMessage = connection.write("subscribe:")
                            // the intent of the flatMap to string is so onError can
                            // be propagated via the concat below
                            .flatMap({ Void t1 ->
                                System.out.println("Send subscribe!");
                                return Observable.empty();
                            });

                    Observable<String> messageHandling = connection.getChannelObservable().map({ ByteBuf bb ->
                        return bb.toString(Charset.forName("UTF8")).trim();
                    });

                    return Observable.concat(subscribeMessage, messageHandling);
                })
                .take(10)
                .toBlockingObservable().forEach({ String v ->
                    System.out.println("Received: " + v);
                });

    }
}
