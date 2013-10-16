package rx.remote;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.StringTokenizer;

import rx.concurrency.Schedulers;
import rx.util.functions.Func2;

public class RemoteServer {

    public static void main(String[] args) {

        try {
            ServerSocket s = new ServerSocket(9876);
            Socket socket = s.accept();
            OutputStream os = socket.getOutputStream();
            InputStream is = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String message = null;
            while ((message = reader.readLine()) != null) {
                System.out.println("Line => " + message);
                executeRemotely(message);
                //                os.write("Sub123456\n".getBytes());
                //                os.flush();
            }

            System.out.println("----------------------------- EXIT");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Object executeRemotely(String message) {
        // do work remotely
        try {
            String[] s = new String[3];
            int i = 0;
            StringTokenizer tk = new StringTokenizer(message, "|");
            while (tk.hasMoreTokens()) {
                String t = tk.nextToken();
                System.out.println("T: " + t);
                s[i++] = t;
            }
            Class c = Class.forName(s[2]);
            Constructor co = c.getDeclaredConstructors()[0];
            co.setAccessible(true);
            if (s[0].equals("Func2")) {
                Func2 f = (Func2) co.newInstance();

                Method m = f.getClass().getMethods()[0];
                Class types[] = m.getParameterTypes();
                for (Class type : types) {
                    System.out.println("Type: " + type);
                }

                if (Integer.class.isAssignableFrom(types[1])) {
                    return f.call(Schedulers.newThread(), Integer.valueOf(s[1]));
                } else if (Long.class.isAssignableFrom(types[1])) {
                    return f.call(Schedulers.newThread(), Long.valueOf(s[1]));
                } else if (Float.class.isAssignableFrom(types[1])) {
                    return f.call(Schedulers.newThread(), Float.valueOf(s[1]));
                } else if (Double.class.isAssignableFrom(types[1])) {
                    return f.call(Schedulers.newThread(), Double.valueOf(s[1]));
                } else {
                    return f.call(Schedulers.newThread(), s[0]);
                }

            } else {
                throw new IllegalStateException("Unknown type: " + s[0]);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
