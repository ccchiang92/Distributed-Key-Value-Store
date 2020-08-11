package kvstore;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static kvstore.KVConstants.ERROR_NO_SUCH_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * @author Chris Chiang
 * Test1 for TPCClientHandler
 * These tests test for basic functionality and edge case error checking for the client handler
 * These test mocks a TPCMaster, so this is a unit test
 * The tests do not use other parts of the project and will work regardless of other part's correctness
 * This test uses many of the same patterns as KVClientTest3 from project3
 * The tests uses parts of the TPCEndToEnd template (only the client side, and the slave servers are not initiated) 
 */

public class TPCClientTest {

    String hostname;
    KVClient client;
    TPCMaster master;
    ServerRunner masterClientRunner;
    ServerRunner masterSlaveRunner;
    HashMap<String, ServerRunner> slaveRunners;

    static final int CLIENTPORT = 8888;
    static final int SLAVEPORT = 9090;

    static final int NUMSLAVES = 4;

    static final long SLAVE1 = 4611686018427387903L;  // Long.MAX_VALUE/2
    static final long SLAVE2 = 9223372036854775807L;  // Long.MAX_VALUE
    static final long SLAVE3 = -4611686018427387903L; // Long.MIN_VALUE/2
    static final long SLAVE4 = -0000000000000000001;  // Long.MIN_VALUE

    static final String KEY1 = "6666666666666666666"; // 2846774474343087985
    static final String KEY2 = "9999999999999999999"; // 8204764838124603412
    static final String KEY3 = "0000000000000000000"; //-7869206253219942869
    static final String KEY4 = "3333333333333333333"; //-2511215889438427442

    @Before
    public void setUp() throws Exception {
        hostname = InetAddress.getLocalHost().getHostAddress();

        startMaster();

        

        client = new KVClient(hostname, CLIENTPORT);
    }

    @After
    public void tearDown() throws InterruptedException {
        masterClientRunner.stop();
        

        client = null;
        master = null;
        slaveRunners = null;
    }

    protected void startMaster() throws Exception {
    	master = mock(TPCMaster.class);
        SocketServer clientSocketServer = new SocketServer(hostname, CLIENTPORT);
        clientSocketServer.addHandler(new TPCClientHandler(master));
        masterClientRunner = new ServerRunner(clientSocketServer, "masterClient");
        masterClientRunner.start();
        Thread.sleep(100);
    }

   
    
    @Test(timeout = 15000)
    public void testPutGet() throws KVException {
    	doNothing().when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.anyBoolean());
        when(master.handleGet(Mockito.isA(KVMessage.class))).thenReturn("bar");
        client.put("foo", "bar");
        assertEquals("get failed", client.get("foo"), "bar");
    
       
    }
    
    //Run Del make sure it doesn't error
    @Test(timeout = 20000)
    public void testDel() {
        try {
        	doNothing().when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.anyBoolean());
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
        	when(master.handleGet(Mockito.isA(KVMessage.class))).thenThrow(new KVException(ERROR_NO_SUCH_KEY));
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
        	doThrow(new KVException(ERROR_NO_SUCH_KEY)).when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.eq(false));
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
        	doNothing().when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.anyBoolean());
        	when(master.handleGet(Mockito.isA(KVMessage.class))).thenThrow(new KVException(ERROR_NO_SUCH_KEY));
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
        	doNothing().when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.anyBoolean());
            when(master.handleGet(Mockito.isA(KVMessage.class))).thenReturn("99AA");
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
        	doNothing().when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.anyBoolean());
            when(master.handleGet(Mockito.isA(KVMessage.class))).thenReturn("COOL");
        	client.put("AWESOME","COOL" );
        	String a =client.get("AWESOME");
        	String b =client.get("AWESOME");
            assertEquals(a, b);
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            fail(errorMsg);
        }
    }

    
    /** Assertions in all valid requests. */
    
    @Test(timeout = 20000)
    public void testAllValidReq() {
        try {
        	doNothing().when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.eq(true));
        	doNothing().when(master).handleTPCRequest(Mockito.isA(KVMessage.class), Mockito.eq(false));
            when(master.handleGet(Mockito.isA(KVMessage.class))).thenReturn("n008");
            client.put("1337","n008" );
            assertEquals("n008", client.get("1337"));
            client.del("1337");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            fail(errorMsg);
        }
        
    }


}
