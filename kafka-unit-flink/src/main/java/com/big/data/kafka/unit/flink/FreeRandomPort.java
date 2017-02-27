package com.big.data.kafka.unit.flink;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Created by kunalgautam on 18.02.17.
 */
public final class FreeRandomPort {

    public static int generateRandomPort() {
        ServerSocket s = null;
        try {
            // ServerSocket(0) results in availability of a free random port
            s = new ServerSocket(0);
            return s.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            assert s != null;
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
