package kvstore;

import static kvstore.KVConstants.*;
import java.io.*;
import java.net.*;
import java.util.Arrays;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;


/**
 * This is the object that is used to create the XML based messages
 * for communication between clients and servers.
 */
public class KVMessage implements Serializable {
    private String msgType;
    private String key;
    private String value;
    private String message;
    private static String requestMessages[] = {GET_REQ, PUT_REQ, DEL_REQ,
                                               COMMIT, ABORT, READY};
    private static String responseMessages[] = {RESP, ACK, REGISTER, SUCCESS}; 
    public static final long serialVersionUID = 6473128480951955693L;

    public KVMessage(KVMessage kvm) {
    	this.msgType=kvm.getMsgType();
    	this.key=kvm.getKey();
    	this.value=kvm.getValue();
    	this.message=kvm.getMessage();
    }

    /**
     * Construct KVMessage with only a type.
     *
     * @param msgType the type of this KVMessage
     */
    public KVMessage(String msgType) {
        this(msgType, null);
    }

    /**L
     * Construct KVMessage with type and message.
     *
     * @param msgType the type of this KVMessage
     * @param message the content of this KVMessage
     */
    public KVMessage(String msgType, String message) {
        this.msgType = msgType;
        this.message = message;
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * Parse XML from the InputStream with unlimited timeout.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock) throws KVException {
        this(sock, 0);
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * This constructor parse XML from the InputStream within a certain timeout
     * or with an unlimited timeout if the provided argument is 0.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @param  timeout total allowable receipt time, in milliseconds
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock, int timeout) throws KVException {
        try {
            sock.setSoTimeout(timeout);
            InputSource source = new InputSource(new NoCloseInputStream(sock.getInputStream()));
            source.setEncoding("UTF-8");
        	NodeList nodeList = 
                DocumentBuilderFactory
                .newInstance()
                .newDocumentBuilder()
                .parse(source)
                .getElementsByTagName("KVMessage");

        	if(nodeList.getLength() <= 0)
        		throw new KVException(ERROR_INVALID_FORMAT);
 
            Element elem = (Element)nodeList.item(0);
            msgType = elem.getAttribute("type");
            NodeList keyList = elem.getElementsByTagName("Key");
    		NodeList valueList = elem.getElementsByTagName("Value");
            NodeList messageList = elem.getElementsByTagName("Message");

            key = (keyList.getLength() > 0) ? keyList.item(0).getTextContent() : null;
            value = (valueList.getLength() > 0) ? valueList.item(0).getTextContent() : null;
            message = (messageList.getLength() > 0) ? messageList.item(0).getTextContent() : null;

            checkMessage(this);

        } catch(SocketTimeoutException e){
       	 	throw new KVException(ERROR_SOCKET_TIMEOUT);
        } catch(IOException e) {
            throw new KVException(ERROR_COULD_NOT_RECEIVE_DATA);
        } catch(ParserConfigurationException | SAXException e) {
            throw new KVException(ERROR_PARSER);
        }
    }

    /**
     * Check if a KVMessage is in a valid format.
     * 
     * @param KVMessage to check.
     * @throws KVException ERROR_INVALID_FORMAT if the message is in valid format.
     */
    private static void checkMessage(KVMessage inMessage) throws KVException {
        boolean fail = false;

        if (inMessage.getMsgType() == null) 
            throw new KVException(ERROR_INVALID_FORMAT);

        switch (inMessage.getMsgType()) {
            case GET_REQ:
                ;     
            case DEL_REQ:
                fail = (inMessage.key == null);
                break;
            case PUT_REQ:
                fail = (inMessage.key == null || inMessage.value == null);
                break;
            case RESP:
                fail = (inMessage.msgType.equals(RESP) && ((inMessage.key != null && inMessage.value == null)
                    || (inMessage.key == null && inMessage.value != null)||(inMessage.key == null 
                    && inMessage.value == null && inMessage.message == null)
                    ||(inMessage.message!=null &&(inMessage.key!=null || inMessage.value!=null))));
                break;
            case REGISTER:
                fail = (inMessage.message == null);
                break;
            case READY:
                ;
            case SUCCESS:
                ;
            case COMMIT:
                ;
            case ACK:
                ;
            case ABORT:
                break;
            default:
                fail = true;
                break;
        }
        if(fail) {
            throw new KVException(ERROR_INVALID_FORMAT);
        }
    }
        
    /**
     * Generate the serialized XML representation for this message. See
     * the spec for details on the expected output format.
     *
     * @return the XML string representation of this KVMessage
     * @throws KVException with ERROR_INVALID_FORMAT or ERROR_PARSER
     */
    public String toXML() throws KVException {
        checkMessage(this);
        try {
            Document doc = 
                DocumentBuilderFactory
                .newInstance()
                .newDocumentBuilder()
                .newDocument();
            
            doc.setXmlStandalone(true);
            Element kvMessageElement = doc.createElement("KVMessage");
            doc.appendChild(kvMessageElement);
            kvMessageElement.setAttribute("type", msgType);
            handleElements(doc, kvMessageElement);
            StringWriter stringWriter = new StringWriter();
            
            TransformerFactory
                .newInstance()
                .newTransformer()
                .transform(new DOMSource(doc), new StreamResult(stringWriter));

            stringWriter.flush();
            return stringWriter.toString();

        } catch(ParserConfigurationException e) {
            throw new KVException(ERROR_PARSER);
        } catch(TransformerException e) {
            throw new KVException(ERROR_PARSER);
        }
    }


    /**
     * Send serialized version of this KVMessage over the network.
     * You must call sock.shutdownOutput() in order to flush the OutputStream
     * and send an EOF (so that the receiving end knows you are done sending).
     * Do not invoke close on the socket. Closing a socket closes the InputStream
     * as well as the OutputStream, preventing the receipt of a response.
     *
     * @param  sock Socket to send XML through
     * @throws KVException with ERROR_INVALID_FORMAT, ERROR_PARSER, or
     *         ERROR_COULD_NOT_SEND_DATA
     */
    public void sendMessage(Socket sock) throws KVException {
        try {
            (new PrintWriter(sock.getOutputStream(), true)).println(this.toXML());
            sock.shutdownOutput();
        } catch(IOException e) {
            throw new KVException(ERROR_COULD_NOT_SEND_DATA);
        } catch(Exception f) {}
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMsgType() {
        return msgType;
    }

    @Override
    public String toString() {
        try {
            return this.toXML();
        } catch (KVException e) {
            return e.toString();
        }
    }

    /*
     * InputStream wrapper that allows us to reuse the corresponding
     * OutputStream of the socket to send a response.
     * Please read about the problem and solution here:
     * http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html
     */
    private class NoCloseInputStream extends FilterInputStream {
        public NoCloseInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() {} // ignore close
    }

    /** 
     * Handle message elements. 
     *
     * @param doc the document created in toxml()
     * @param kvMessageElement The KVMessage element in doc 
     * @throws KVException if message is of an unrecognized type.
     */
    private void handleElements(Document doc, Element kvMessageElement) throws KVException {
        String[] has_key = {PUT_REQ, RESP, DEL_REQ, GET_REQ}; 
        String[] has_value = {RESP, PUT_REQ}; 
        String[] has_message = {READY, REGISTER, RESP, ABORT, ACK, COMMIT}; 
        boolean recognized = false;

        if(Arrays.asList(has_key).contains(msgType)) {
            recognized = true;
            if(key != null) {
                Element keyElement = doc.createElement("Key");
                keyElement.appendChild(doc.createTextNode(key));
                kvMessageElement.appendChild(keyElement);
            }
        }
        if(Arrays.asList(has_value).contains(msgType)) {
            recognized = true;
            if(value != null) {
                Element valueElement = doc.createElement("Value");
                valueElement.appendChild(doc.createTextNode(value));
                kvMessageElement.appendChild(valueElement);
            }
        }
        if(Arrays.asList(has_message).contains(msgType)) {
            recognized = true;
            if(message != null) {
                Element messageElement = doc.createElement("Message");
                messageElement.appendChild(doc.createTextNode(message));
                kvMessageElement.appendChild(messageElement);
            }
        }
        if(!recognized) {
            System.out.println(msgType + " is not recognized");
            throw new KVException(ERROR_PARSER);
        }
    }
}
