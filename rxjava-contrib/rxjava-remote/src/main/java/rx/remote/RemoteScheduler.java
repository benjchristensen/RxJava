package rx.remote;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import rx.Scheduler;
import rx.Subscription;
import rx.subscriptions.Subscriptions;
import rx.util.functions.Action0;
import rx.util.functions.Func2;

public class RemoteScheduler {

    private final Scheduler remoteScheduler;

    public RemoteScheduler(URI remoteServer) {
        this.remoteScheduler = new NetworkedScheduler(remoteServer);

    }

    @Override
    public <T> Subscription schedule(T state, Func2<? super Scheduler, ? super T, ? extends Subscription> action) {
        return remoteScheduler.schedule(state, action);
    }

    @Override
    public <T> Subscription schedule(T state, Func2<? super Scheduler, ? super T, ? extends Subscription> action, long delayTime, TimeUnit unit) {
        return remoteScheduler.schedule(state, action, delayTime, unit);
    }

    private static class NetworkedScheduler extends Scheduler {

        private final URI uri;

        NetworkedScheduler(URI uri) {
            this.uri = uri;
        }

        @Override
        public <T> Subscription schedule(T state, Func2<? super Scheduler, ? super T, ? extends Subscription> action) {
            try {
                // TODO make this all async and handle resources!!
                Socket socket = new Socket(uri.getHost(), uri.getPort());
                System.out.println("Open socket: " + socket);
                OutputStream os = socket.getOutputStream();
                String message = "Func2|" + state + "|" + action.getClass().getName();
                System.out.println("Message: " + message);
                os.write(message.getBytes());
                os.write('\n');
                os.flush();
                
                System.out.println("wrote to network");
                InputStream is = socket.getInputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(is));
                System.out.println("waiting for response");
                final String response = in.readLine();
                System.out.println("Recevied response: " + response);
                try {
                    in.close();
                    is.close();
                    os.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return Subscriptions.create(new Action0() {

                    @Override
                    public void call() {
                        System.out.println("**** would unsubscribe here over network: " + response);
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException("Unable to communicate over socket: " + uri, e);
            }
        }

        @Override
        public <T> Subscription schedule(T state, Func2<? super Scheduler, ? super T, ? extends Subscription> action, long delayTime, TimeUnit unit) {
            // TODO Auto-generated method stub
            return null;
        }

    };
}
