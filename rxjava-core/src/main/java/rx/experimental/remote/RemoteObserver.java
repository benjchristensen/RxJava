package rx.experimental.remote;

import rx.Observable;

public class RemoteObserver<T> {

    public Observable<Void> onNext(T t) {
        return Observable.empty(); // success
    }

    public Observable<Void> onError(Throwable t) {
        return Observable.empty(); // success
    }

    public Observable<Void> onCompleted() {
        return Observable.empty(); // success
    }
}
