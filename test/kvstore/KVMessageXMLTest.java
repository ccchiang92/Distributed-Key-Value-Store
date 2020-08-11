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
 * These tests test the toXML method in KVMessage.
 * They test that the method functions correctly and serializes KVMessage
 * the Serialize message are printed, but they are also read through a KVMessage
 * deserialize constructor where a socket is mocked.
 * After deserializing, the fields of the message are asserted.
 * The tests also tests that the method catches errors correctly
 *
 */
public class KVMessageXMLTest {

    /** Variables for testing. */
    final String PUT_REQUEST = "PUT_REQUEST";
    final String GET_REQUEST = "GET_REQUEST";
    final String DEL_REQUEST = "DEL_REQUEST";
    final String PUT_SUCCESS = "PUT_SUCCESS";
    final String GET_SUCCESS = "GET_SUCCESS";
    final String DEL_SUCCESS = "DEL_SUCCESS";
    final String R_UNSUCCESS = "R_UNSUCCESS";

	//Test serializing a put request
    @Test
    public void toXMLPutReq() throws KVException {
    	KVMessage testKvm = new KVMessage(PUT_REQ);
    	testKvm.setValue("10");
    	testKvm.setKey("abc");
    	InputStream f = new StringBufferInputStream(testKvm.toXML());
    	System.out.println(testKvm.toXML());
    	Socket mockSock = mock(Socket.class);
    	try {
    		doNothing().when(mockSock).setSoTimeout(anyInt());
    		when(mockSock.getInputStream()).thenReturn(f);
    	} catch (IOException e) {
            throw new RuntimeException(e);
        }
        KVMessage kvm = new KVMessage(mockSock);
        assertNotNull(kvm);
        assertEquals(PUT_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertEquals("abc",kvm.getKey());
        assertEquals("10", kvm.getValue());
    }

  //Test serializing a get request
    @Test
    public void toXMLGetReq() throws KVException {
    	KVMessage testKvm = new KVMessage(GET_REQ);
    	testKvm.setKey("abc");
    	InputStream f = new StringBufferInputStream(testKvm.toXML());
    	System.out.println(testKvm.toXML());
    	Socket mockSock = mock(Socket.class);
    	try {
    		doNothing().when(mockSock).setSoTimeout(anyInt());
    		when(mockSock.getInputStream()).thenReturn(f);
    	} catch (IOException e) {
            throw new RuntimeException(e);
        }
        KVMessage kvm = new KVMessage(mockSock);
        assertNotNull(kvm);
        assertEquals(GET_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertEquals("abc",kvm.getKey());
        assertNull(kvm.getValue());
    }

    //Test serializing a del request
    @Test
    public void toXMLDelReq() throws KVException {
    	KVMessage testKvm = new KVMessage(DEL_REQ);
    	testKvm.setKey("ff12355555555555555555555555555555555");
    	InputStream f = new StringBufferInputStream(testKvm.toXML());
    	System.out.println(testKvm.toXML());
    	Socket mockSock = mock(Socket.class);
    	try {
    		doNothing().when(mockSock).setSoTimeout(anyInt());
    		when(mockSock.getInputStream()).thenReturn(f);
    	} catch (IOException e) {
            throw new RuntimeException(e);
        }
        KVMessage kvm = new KVMessage(mockSock);
        assertNotNull(kvm);
        assertEquals(DEL_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertEquals("ff12355555555555555555555555555555555", kvm.getKey());
        assertNull(kvm.getValue());
    }

    //Test serializing a resp with a message field
    @Test
    public void toXMLResp() throws KVException {
    	KVMessage testKvm = new KVMessage(RESP);
    	testKvm.setMessage(ERROR_NO_SUCH_KEY);
    	InputStream f = new StringBufferInputStream(testKvm.toXML());
    	System.out.println(testKvm.toXML());
    	Socket mockSock = mock(Socket.class);
    	try {
    		doNothing().when(mockSock).setSoTimeout(anyInt());
    		when(mockSock.getInputStream()).thenReturn(f);
    	} catch (IOException e) {
            throw new RuntimeException(e);
        }
        KVMessage kvm = new KVMessage(mockSock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertEquals(ERROR_NO_SUCH_KEY, kvm.getMessage());
        assertNull(kvm.getKey());
        assertNull(kvm.getValue());
    }

    //Test serializing a put req with a message field
    @Test
    public void toXMLToManyFields() throws KVException {
    	KVMessage testKvm = new KVMessage(PUT_REQ);
    	testKvm.setValue("10");
    	testKvm.setKey("abc");
    	testKvm.setMessage("abc");
    	InputStream f = new StringBufferInputStream(testKvm.toXML());
    	System.out.println(testKvm.toXML());
    	Socket mockSock = mock(Socket.class);
    	try {
    		doNothing().when(mockSock).setSoTimeout(anyInt());
    		when(mockSock.getInputStream()).thenReturn(f);
    	} catch (IOException e) {
            throw new RuntimeException(e);
        }
        KVMessage kvm = new KVMessage(mockSock);
        assertNotNull(kvm);
        assertEquals(PUT_REQ, kvm.getMsgType());
        assertEquals("abc",kvm.getKey());
        assertEquals("10",kvm.getValue());
    }

    //Test for formatting error check, invalid type
    @Test
    public void toXMLErrorCheck() {
    	KVMessage testKvm = new KVMessage("abcde");
    	try{
    	    InputStream f = new StringBufferInputStream(testKvm.toXML());
    	    fail("did not throw exception");
    	    System.out.println(testKvm.toXML());
    	} catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_FORMAT);
    	}
    }

    //Test for formatting error check, null fields for resp
    @Test
    public void toXMLErrorCheck2() {
    	KVMessage testKvm = new KVMessage("RESP");
    	try{
    	    InputStream f = new StringBufferInputStream(testKvm.toXML());
    	    fail("did not throw exception");
    	    System.out.println(testKvm.toXML());
    	} catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_FORMAT);
    	}
    }

    //Test for formatting error check, null fields for get
    @Test
    public void toXMLErrorCheck3() {
    	KVMessage testKvm = new KVMessage("GET_REQ");
    	try {
    	    InputStream f = new StringBufferInputStream(testKvm.toXML());
    	    fail("did not throw exception");
    	    System.out.println(testKvm.toXML());
    	} catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_FORMAT);
    	}
    }

    /** Couple of tests for expected XML strings.
     *  Note: we're testing the format, so no need to know whether the message
     *  is Success or not.
     *  @throws KVException if there is some invalid format or a parsing error.
     *  */
    @Test
    public void testValidXMLFormats() throws KVException {
        KVMessage testKvm;
        String key = "hello";
        String value = "world";
        String message = "foobar";

        /** Put Request. */
        testKvm = new KVMessage(PUT_REQ);
        testKvm.setKey(key);
        testKvm.setValue(value);
        assertEquals(
                expectedXMLString(PUT_REQUEST, key, value, message),
                testKvm.toXML());

        /** Get Request. */
        testKvm = new KVMessage(GET_REQ);
        testKvm.setKey(key);
        assertEquals(
                expectedXMLString(GET_REQUEST, key, value, message),
                testKvm.toXML());

        /** Del Request. */
        testKvm = new KVMessage(DEL_REQ);
        testKvm.setKey(key);
        assertEquals(
                expectedXMLString(DEL_REQUEST, key, value, message),
                testKvm.toXML());

        /** Successful Put. */
        testKvm = new KVMessage(RESP, message);
        assertEquals(
                expectedXMLString(PUT_SUCCESS, key, value, message),
                testKvm.toXML());

        /** Successful Get. */
        testKvm = new KVMessage(RESP);
        testKvm.setKey(key);
        testKvm.setValue(value);
        assertEquals(
                expectedXMLString(GET_SUCCESS, key, value, message),
                testKvm.toXML());

        /** Successful Del. */
        testKvm = new KVMessage(RESP, message);
        assertEquals(
                expectedXMLString(PUT_SUCCESS, key, value, message),
                testKvm.toXML());

        /** Unsuccessful Response. */
        testKvm = new KVMessage(RESP, message);
        assertEquals(
                expectedXMLString(PUT_SUCCESS, key, value, message),
                testKvm.toXML());
    }

    /** Couple of tests for invalid formats.
     *  @throw KVException if there are invalid formats. */
    public void testInvalidXMLFormats() {
        /** No message type. */

    }


    /** Expected XML string for testing.
     *  @param reqOrResp PUTREQUEST, GETREQUEST, DELREQUEST,
     *                   PUTSUCCESS, GETSUCCESS, DELSUCCESS, RESPERROR (these
     *                   are not actually potential types; we merely use these
     *                   params for ease of usability and coding).
     *  @param key The key in the XML file.
     *  @param value The value in the XML file.
     *  @param message The message in the XML file.
     *  @return the expected XML string. */
    public String expectedXMLString(String reqOrResp, String key, String value,
                            String message) {
        String xmlString = "";
        if (reqOrResp == PUT_REQUEST) {
            xmlString += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<KVMessage type=\"putreq\">"
                    + "<Key>" + key + "</Key>"
                    + "<Value>" + value + "</Value>"
                    + "</KVMessage>";
        } else if (reqOrResp == GET_REQUEST) {
            xmlString += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<KVMessage type=\"getreq\">"
                    + "<Key>" + key + "</Key>"
                    + "</KVMessage>";
        } else if (reqOrResp == DEL_REQUEST) {
            xmlString += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<KVMessage type=\"delreq\">"
                    + "<Key>" + key + "</Key>"
                    + "</KVMessage>";
        } else if (reqOrResp == PUT_SUCCESS || reqOrResp == DEL_SUCCESS
                || reqOrResp == R_UNSUCCESS) {
            xmlString += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<KVMessage type=\"resp\">"
                    + "<Message>" + message + "</Message>"
                    + "</KVMessage>";
        } else if (reqOrResp == GET_SUCCESS) {
            xmlString += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<KVMessage type=\"resp\">"
                    + "<Key>" + key + "</Key>"
                    + "<Value>" + value + "</Value>"
                    + "</KVMessage>";
        }
        return xmlString;
    }



}
