package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.net.*;
import java.util.*;

import org.junit.*;
/**
 * 
 * @author Chris Chiang
 *	This Test is a simple test case of send message
 *  Which creates a socket directly without a client
 */
public class KVMessageSendTest {
	
	int NUM_THREADS = 100; 
    String hostname;
    ServerRunner serverRunner;
    Socket sock;
    
    //Setting up the socketServer connection using endToEnd Template
    @Before
    public void setUp() throws IOException, InterruptedException {
        String hostname = InetAddress.getLocalHost().getHostAddress();

        SocketServer ss = new SocketServer(hostname, 8080);
        ss.addHandler(new ServerClientHandler(new KVServer(100, 10),NUM_THREADS));
        serverRunner = new ServerRunner(ss, "server");
        serverRunner.start();
        try {
        	sock= new Socket(hostname, 8080);//directly create a socket without using a client
        } catch(Exception e) {
        	throw new RuntimeException(e);        	
        }
    }

    @After
    public void tearDown() throws InterruptedException,IOException {
    	sock.close();
        serverRunner.stop();
    }

    @Test
    public void SendMessageTest() throws KVException {
    	//First a Sample Message is created
    	KVMessage testKvm=new KVMessage(PUT_REQ);
    	testKvm.setValue("10");
    	testKvm.setKey("abc");
    	//The validity of the message gets checked
        assertNotNull(testKvm);
        assertEquals(PUT_REQ, testKvm.getMsgType());
        assertNull(testKvm.getMessage());
        assertEquals("abc",testKvm.getKey());
        assertEquals("10",testKvm.getValue());
        testKvm.sendMessage(sock);//message is sent
        KVMessage respKvm=new KVMessage(sock);//a respond from server is created
        assertNotNull(respKvm);
        assertEquals(RESP, respKvm.getMsgType());
        assertEquals(SUCCESS, respKvm.getMessage());
        //check response is successful
        
    }
    
   


}
