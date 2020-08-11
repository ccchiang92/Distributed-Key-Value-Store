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
 * This test uses the template given in KVMessageTest
 * The first two tests are the exact copies of the tests in KVMessageTest
 * The mocking helper method is left untouched as well
 * These tests use various txt input files for serialized messages
 * These tests test the functionality of KVMessage constructor using a socket as argument
 * This also check error handling of the constructor
 * 
 */
public class KVMessageDeSerializeTest {

    private Socket sock;

    private static final String TEST_INPUT_DIR = "test/kvstore/test-inputs/";
    
    
    //Parse a valid Put
    @Test
    public void successfullyParsesPutReq() throws KVException {
        setupSocket("putreq.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(PUT_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
        assertNotNull(kvm.getValue());
    }
    //Parse a valid Del
    @Test
    public void successfullyParsesdelReq() throws KVException {
        setupSocket("delreq.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(DEL_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
        assertNull(kvm.getValue());
    }
    //Parse a valid Get
    @Test
    public void successfullyParsesGetReq() throws KVException {
        setupSocket("getreq.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(GET_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
    }

    //Parse a valid RESP
    @Test
    public void successfullyParsesPutResp() throws KVException {
        setupSocket("putresp.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertEquals("Success", kvm.getMessage());
        assertNull(kvm.getKey());
        assertNull(kvm.getValue());
    }
    //Parse a RESP with error message
    @Test
    public void successfullyParsesFailResp() throws KVException {
        setupSocket("respFail.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertEquals(ERROR_NO_SUCH_KEY, kvm.getMessage());
        assertNull(kvm.getKey());
        assertNull(kvm.getValue());
    }
    
    //Error Check-no key
    @Test
    public void ParsesPutNoKey() {
        setupSocket("putNoKey.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error Check-no value
    @Test
    public void ParsesPutNoValue() {
        setupSocket("putNoValue.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error Check-no key
    @Test
    public void ParsesGetNoKey() {
        setupSocket("getNoKey.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error Check-no key
    @Test
    public void ParsesDelNoKey() {
        setupSocket("delNoKey.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error Check-no msgType
    @Test
    public void NoType() {
        setupSocket("NoType.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error Check-invalid msgTyep
    @Test
    public void WrongType() {
        setupSocket("WrongType.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error check-Too Many fields in Resp
    @Test
    public void RespMany() {
        setupSocket("respMany.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error check-Only Key in Resp
    @Test
    public void RespKeyOnly() {
        setupSocket("respOnlyKey.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error check-Only value in Resp
    @Test
    public void RespValueOnly() {
        setupSocket("respOnlyValue.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error check-field names are wrong
    @Test
    public void WrongFieldNames() {
        setupSocket("WrongNames.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        	}
    }
    
    //Error Check-parsing in an empty txt
    @Test
    public void Empty() {
        setupSocket("Empty.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_PARSER);
        	}
    }
    
    //Error Check-parsing a file where the xml format is incorrect 
    @Test
    public void MessUpMessage() {
        setupSocket("messUpResp.txt");
        try{
        KVMessage kvm = new KVMessage(sock);
        fail("did not throw error");
        	}
        catch (KVException kve) {
                String errorMsg = kve.getKVMessage().getMessage();
                assertEquals(errorMsg, ERROR_PARSER);
        	}
    }
    
    /**Check Time out exception and others**/

    /* Begin helper methods */

    private void setupSocket(String filename) {
        sock = mock(Socket.class);
        File f = new File(System.getProperty("user.dir"), TEST_INPUT_DIR + filename);
        try {
            doNothing().when(sock).setSoTimeout(anyInt());
            when(sock.getInputStream()).thenReturn(new FileInputStream(f));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
