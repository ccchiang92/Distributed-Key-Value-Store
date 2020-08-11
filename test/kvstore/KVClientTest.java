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



/**
 * @author Chris Chiang
 * Unit tests for KVClient
 * These only tests for edge case inputs that throws Exceptions
 * These tests does not require any other tasks to pass
 * sockerServer and KVMessages are not used in these tests
 */
public class KVClientTest {

	private  KVClient client;
	String hostname;
	@Before
	public void setUp() throws Exception {
        try {
            hostname = InetAddress.getLocalHost().getHostAddress();
            client = new KVClient(hostname, 8080);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
	}
	//Test invalid key for put
    @Test(timeout = 20000)
    public void testInvalidPutKey() {
        try {
            client.put("", "bar");
            fail("Didn't fail on null key");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_KEY);
        }
    }

    //Test invalid value for put
    @Test(timeout = 20000)
    public void testInvalidPutValue() {
        try {
            client.put("abc", "");
            fail("Didn't fail on null value");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_VALUE);
        }
    }

    //Test invalid key for get
    @Test(timeout = 20000)
    public void testInvalidGetKey() {
        try {
            client.get("");
            fail("Didn't fail on null key");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_KEY);
        }
    }

  //Test invalid key for del
    @Test(timeout = 20000)
    public void testInvalidDelKey() {
        try {
            client.del("");
            fail("Didn't fail on null key");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_KEY);
        }
    }
    //Test when trying to connect an non-existing host
    @Test(timeout = 20000)
    public void testUnknownHost() {
        try {
        	KVClient failedClient = new KVClient("12333456887asdjasdklj", 5000);
        	failedClient.get("abcd");
        } catch (KVException kve) {
        	String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_COULD_NOT_CONNECT);
        }
    }

    //Test when trying to connect the wrong ports
    @Test(timeout = 20000)
    public void testSocketIO() {
        try {
        	KVClient failedClient = new KVClient(hostname, 0);
        	failedClient.get("abcd");

        } catch (KVException kve) {
        	String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_COULD_NOT_CREATE_SOCKET);
        }
	}



}
