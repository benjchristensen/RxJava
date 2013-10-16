package rx.remote.examples;

import java.net.URI;
import java.net.URISyntaxException;

import rx.Observable;
import rx.Observable.OnSubscribeFunc;
import rx.Observer;
import rx.Subscription;
import rx.concurrency.Schedulers;
import rx.remote.RemoteScheduler;
import rx.util.functions.Action1;
import rx.util.functions.Func1;

public class RxRemoteQuery {

    public static void main(String[] args) {

        try {
            RemoteScheduler REMOTE_SCHEDULER = new RemoteScheduler(new URI("tcp://127.0.0.1:9876"));

            Observable.range(1, 1000).observeOn(REMOTE_SCHEDULER).map(new Func1<Integer, String>() {

                @Override
                public String call(Integer t1) {
                    // expensive work done here
                    return "ValueAfterProcessing: " + t1;
                }

            }).take(10).observeOn(Schedulers.currentThread()).subscribe(onNext);

            
            
            
            
            Observable.range(1, 1000).remote(new Func1<Observable<T>, Observable<S>>() {

                @Override
                public Observable<S> call(Observable<T> t1) {
                    return t1.map(new Func1<Integer, String>() {

                        @Override
                        public String call(Integer t1) {
                            // expensive work done here
                            return "ValueAfterProcessing: " + t1;
                        }

                    }).take(10);
                }
            }, REMOTE_SCHEDULER).observeOn(Schedulers.currentThread()).subscribe(onNext);

            Observable.create(new OnSubscribeFunc<String>() {

                @Override
                public Subscription onSubscribe(Observer<? super String> observer) {
                    System.out.println("Hello World!");

                    return null;
                }
            }).subscribeOn(REMOTE_SCHEDULER)
                    .parallel(new Func1<Observable<String>, Observable<String>>() {

                        @Override
                        public Observable<String> call(Observable<String> t1) {
                            // TODO Auto-generated method stub
                            return null;
                        }

                    }, Schedulers.threadPoolForComputation()).observeOn(Schedulers.currentThread()).subscribe(new Action1<String>() {

                        @Override
                        public void call(String t1) {
                            // TODO Auto-generated method stub

                        }

                    });

            //            r.schedule(100, new Func2<Scheduler, Integer, Subscription>() {
            //
            //                @Override
            //                public Subscription call(Scheduler s, final Integer state) {
            //                    return s.schedule(new Action1<Action0>() {
            //                        int i = state;
            //
            //                        @Override
            //                        public void call(Action0 self) {
            //                            if (i-- > 0) {
            //                                System.out.println("Hello " + i + "!");
            //                                self.call();
            //                            }
            //                        }
            //                    });
            //
            //                }
            //            }).unsubscribe();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
}
