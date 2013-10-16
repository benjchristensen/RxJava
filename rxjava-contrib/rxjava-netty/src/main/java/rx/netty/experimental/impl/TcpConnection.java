package rx.netty.experimental.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.Subscriptions;
import rx.util.functions.Action0;
import rx.util.functions.Action1;

public class TcpConnection {

    private final PublishSubject<ByteBuf> s;
    private final ChannelHandlerContext ctx;
    private volatile CompositeSubscription subscriptions = new CompositeSubscription();

    protected TcpConnection(ChannelHandlerContext ctx, final PublishSubject<ByteBuf> s) {
        this.ctx = ctx;
        this.s = s;
    }

    public Observable<ByteBuf> getChannelObservable() {
        return s;
    }

    /* package */Observer<ByteBuf> getChannelObserver() {
        return new Observer<ByteBuf>() {
            public synchronized void onCompleted() {
                s.onCompleted();
                subscriptions.unsubscribe();
            }

            public synchronized void onError(Throwable e) {
                s.onError(e);
                subscriptions.unsubscribe();
            }

            public void onNext(ByteBuf o) {
                s.onNext(o);
            }
        };
    }

    public static TcpConnection create(ChannelHandlerContext ctx) {
        return new TcpConnection(ctx, PublishSubject.<ByteBuf> create());
    }

    public synchronized void unsubscribe() {
        subscriptions.unsubscribe();
        subscriptions = new CompositeSubscription();
    }

    public synchronized void addSubscription(Subscription s) {
        subscriptions.add(s);
    }

    public Observable<Void> write(final String msg) {
        return write(Unpooled.wrappedBuffer(msg.getBytes()));
    }

    public Observable<Void> write(final byte[] msg) {
        return write(Unpooled.wrappedBuffer(msg));
    }

    /**
     * Note that this eagerly subscribes. It is NOT lazy.
     * 
     * @param msg
     * @return
     */
    public Observable<Void> write(final ByteBuf msg) {
        Observable<Void> o = Observable.create(new OnSubscribeFunc<Void>() {

            @Override
            public Subscription onSubscribe(final Observer<? super Void> observer) {
                final ChannelFuture f = ctx.writeAndFlush(msg);

                f.addListener(new GenericFutureListener<Future<Void>>() {

                    @Override
                    public void operationComplete(Future<Void> future) throws Exception {
                        if (future.isSuccess()) {
                            observer.onCompleted();
                        } else {
                            observer.onError(future.cause());
                        }
                    }
                });

                return Subscriptions.create(new Action0() {

                    @Override
                    public void call() {
                        f.cancel(true);
                    }

                });
            }
        }).cache();

        o.subscribe(new Action1<Void>() {

            @Override
            public void call(Void t1) {
                // do nothing, we are eagerly executing the Observable
            }
        }, new Action1<Throwable>() {

            @Override
            public void call(Throwable cause) {
                System.out.println("cached error: " + cause);
                // do nothing, it will be cached for the other subscribers
            }

        });

        // return Observable that is multicast (cached) so it can be subscribed to it wanted
        return o;

    }

}
