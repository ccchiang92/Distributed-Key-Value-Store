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

import static org.mockito.Mockito.*;


/**
 * @author Chris Chiang
 * Test for TPCClientHandler
 * These tests does not mock and uses other parts of the project
 * so the tests will only work if those other pieces are working correctly
 * These tests test for basic functionality and edge case error checking on the client side
 * This test is almost the same as KVClientTest3 from project3
 * The only difference being it uses TPCEndToEnd Template instead of just EndToEnd Template
 */

public class TPCClientTest2 extends TPCEndToEndTemplate {

	//A simple put/get test asserts we get what we puted
    @Test(timeout = 20000)
    public void SimplePutGet() {
        try {
            client.put("5", "99");
            assertEquals(client.get("5"), "99");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            fail(errorMsg);
        }
    }

    //Run Del make sure it doesn't error
    @Test(timeout = 20000)
    public void testDel() {
        try {
            client.put("abc", "efg");
            client.del("abc");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            fail(errorMsg);
        }
    }

    //Getting with a non-existing key
    @Test(timeout = 20000)
    public void testInvalidGetKey() {
        try {
            client.get("10");
            fail("Didn't fail on key that's not on server");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_NO_SUCH_KEY);
        }
    }
    //Tries to Del with a non-existing key
    @Test(timeout = 20000)
    public void testInvalidDelKey() {
        try {
            client.del("100");
            fail("Didn't fail on key that's not on server");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_NO_SUCH_KEY);
        }
    }

    //Del functionality test, make sure after del, the key/value is non-existent
    @Test(timeout = 20000)
    public void DelTest2() {
        try {
        	client.put("100","50" );
            client.del("100");
            client.get("100");
            fail("Didn't fail on key that's not on server");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_NO_SUCH_KEY);
        }
    }

    //Calls put twice, makes sure the value overwrites
    @Test(timeout = 20000)
    public void DoublePut() {
        try {
        	client.put("ZZZ","50" );
        	client.put("ZZZ", "99AA");
            assertEquals(client.get("ZZZ"), "99AA");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            fail(errorMsg);
        }
    }

    //Calls get twice, makes sure the value are the same
    @Test(timeout = 20000)
    public void DoubleGET() {
        try {
        	client.put("AWESOME","COOL" );
        	String a =client.get("AWESOME");
        	String b =client.get("AWESOME");
            assertEquals(a, b);
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            fail(errorMsg);
        }
    }

    /** Asserts that two DEL_REQS will lead to invalidty. */
    @Test(timeout = 20000)
    public void testTwoDelReq() {
        try {
            client.put("Heaven","Piercing" );
            assertEquals("Piercing", client.get("Heaven"));
            client.del("Heaven");
            client.del("Heaven");
            fail("Two del requests are not being handled correctly.");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(ERROR_NO_SUCH_KEY, errorMsg);
        }
    }

    /** Assertions in all valid requests. */
    @Test(timeout = 20000)
    public void testAllValidReq() {
        try {
            client.put("1337","n008" );
            assertEquals("n008", client.get("1337"));
            client.del("1337");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            fail(errorMsg);
        }
    }


}
