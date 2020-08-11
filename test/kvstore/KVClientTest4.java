package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;



/** Tests for errors in CONNECTHOST() within KVClient.java. */
public class KVClientTest4 {

    private KVClient clientInvalidSocket;
    private KVClient clientNullHostName;
    private KVClient clientUnknownHostName;
    private KVClient clientIOException;
    String hostname;
    @Before
    public void setUp() throws Exception {
        try {
            hostname = InetAddress.getLocalHost().getHostAddress();
            clientInvalidSocket = new KVClient(hostname, -1);
            clientNullHostName = new KVClient(null, 2);
            clientUnknownHostName = new KVClient("PIERCE THE HEAVENS", 3);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Tests for invalid sockets i.e. portnum < 0 or portnum > 65535. */
    @Test(timeout = 20000)
    public void testInvalidSocketError() {
        try {
            clientInvalidSocket.put("Ace", "Ventura");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(ERROR_COULD_NOT_CONNECT, errorMsg);
        }
    }

    /** Tests for invalid hostname, specifically null. */
    @Test(timeout = 20000)
    public void testNullException() {
        try {
            clientNullHostName.put("Mad", "Scientist");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(ERROR_COULD_NOT_CREATE_SOCKET, errorMsg);
        }
    }

    /** Tests for invalid hostname, specifically some unknown host name. */
    @Test(timeout = 20000)
    public void testUnknownHostName() {
        try {
            clientUnknownHostName.put("Yuck", "Foo");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(ERROR_COULD_NOT_CONNECT, errorMsg);
        }
    }
}
