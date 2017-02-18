package com.big.data.kafka.unit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Random;


public final class PortAvailabilityUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(PortAvailabilityUtils.class);

    private static final int MIN_RANDOM_PORT = 0xc000;
    private static final int MAX_RANDOM_PORT = 0xfffe;
    private static Random random = new Random();

    private PortAvailabilityUtils(){
        //do nothing
    }

    /**
     * Checks to see if a specific port is available(Bind is not called). setReuseAddress has been enabled to have port
     * reusability in case of TIME_WAIT.
     *
     * @param port the port number to check for availability
     * @return <tt>true</tt> if the port is available, or <tt>false</tt> if not
     */
    public static boolean available(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            LOGGER.info("Execption caught while finding port", e);
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    LOGGER.info("Execption caught while finding port", e);
                }
            }
        }

        return false;
    }


    /**
     * Returns a random port. These ports cannot be registered with IANA and are intended for dynamic allocation (see
     * http://bit.ly/dynports).
     */
    public static int randomPort() {
        return MIN_RANDOM_PORT + random.nextInt(MAX_RANDOM_PORT - MIN_RANDOM_PORT);
    }

    /**
     * Returns a random free port and marks that port as taken. Not thread-safe. Expected to be called from
     * single-threaded test setup code nc -l portno can be used to verify the results
     */
    public static int randomFreePort() {
        int port ;
        boolean portAvailable;
        do {
            port = randomPort();
            portAvailable = available(port);
        } while (!portAvailable);
        return port;
    }




    public static String randomMultiCastAddress() {
        return "226.1.1." + random.nextInt(254);
    }

    public static void main(String[] args) {
        LOGGER.info( "free port is {}", PortAvailabilityUtils.randomFreePort());
    }





}
