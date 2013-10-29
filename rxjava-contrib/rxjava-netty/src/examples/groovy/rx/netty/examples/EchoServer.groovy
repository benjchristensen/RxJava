package rx.netty.examples

import rx.Observable
import rx.Subscription
import rx.netty.experimental.RxNetty
import rx.netty.experimental.impl.TcpConnection
import rx.netty.experimental.protocol.ProtocolHandlers

class EchoServer {

    def static void main(String[] args) {

        Subscription s = RxNetty.createTcpServer(8181, ProtocolHandlers.stringCodec())
                .onConnect({ TcpConnection<String, String> connection ->
                    // writing to the connection is the only place where anything is remote
                    connection.write("Welcome! \n\n")

                    // perform echo logic and return the transformed output stream that will be subscribed to
                    return connection.getChannelObservable().flatMap({ String msg ->
                        // echo the input to the output stream
                        String s = msg.trim()
                        if(s.isEmpty()) {
                            // we skip empty messages
                            return Observable.empty()
                        } else {
                            connection.write("echo => " + s + "\n")
                            return Observable.just(s)
                        }
                    });
                }).toBlockingObservable().forEach({ String o ->
                    println("onNext: " + o)
                });

    }
}
