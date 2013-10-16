package rx.netty.experimental.impl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import rx.Observer;

public class HandlerObserver extends ChannelInboundHandlerAdapter {

    private final Observer<? super TcpConnection> observer;
    private volatile TcpConnection connection;

    public HandlerObserver(Observer<? super TcpConnection> observer) {
        this.observer = observer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            connection.getChannelObserver().onNext((ByteBuf) msg);
        } else {
            connection.getChannelObserver().onError(new RuntimeException("Unexpected message type: " + msg));
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (connection != null) {
            connection.getChannelObserver().onError(cause);
        } else {
            observer.onError(new RuntimeException("Error occurred and connection does not exist: " + cause));
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        connection.getChannelObserver().onCompleted();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        connection = TcpConnection.create(ctx);
        observer.onNext(connection);
    }

}
