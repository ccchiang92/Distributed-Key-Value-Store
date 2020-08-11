package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Client API used to send requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

    private String server;
    private int port;
    private int socketTimeout;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port on which the server is listening
     */
    public KVClient(String server, int port, int socketTimeout) {
        this.server = server;
        this.port = port;
        this.socketTimeout = socketTimeout;
    }

    public KVClient(String server, int port) {
        this(server, port, 0);
    }

    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to create or connect socket
     */
    private Socket connectHost() throws KVException {
        // implement me
        try {
            return new Socket(server, port);
        } catch(UnknownHostException e) {
            throw new KVException(ERROR_COULD_NOT_CONNECT);
        } catch(IOException f) {
            throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
        } catch (IllegalArgumentException g) {
            throw new KVException(ERROR_COULD_NOT_CONNECT);
        }
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param  sock Socket to be closed
     */
    private void closeHost(Socket sock) {
        // implement me
        try {
            sock.close();
        } catch(Exception e) {
            return;
        }
    }

    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
        if (key == null || key.equals("")) {
            throw new KVException(ERROR_INVALID_KEY);
        } else if (value == null || value.equals("")) {
            throw new KVException(ERROR_INVALID_VALUE);
        }
        KVMessage receiveKVMsg = sendMessage(PUT_REQ, key, value);

        if (receiveKVMsg.getMessage() == null) {
            throw new KVException(ERROR_INVALID_FORMAT);
        }
        else if (!receiveKVMsg.getMessage().equals(SUCCESS)){
            throw new KVException (receiveKVMsg.getMessage());
        }
    }

    /**
     * Issues a GET request to the server.
     *
     * @param  key String to get value for in server
     * @return String value associated with key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public String get(String key) throws KVException {
        if (key == null || key.equals("")) {
            throw new KVException(ERROR_INVALID_KEY);
        }
        Socket sock = connectHost();
        try {
            KVMessage sendKVMsg = new KVMessage(GET_REQ);
            sendKVMsg.setKey(key);
            sendKVMsg.sendMessage(sock);
            KVMessage receiveKVMsg = new KVMessage(sock);
            if (receiveKVMsg.getMessage() == null) {
            	if (receiveKVMsg.getKey() == null
            			|| receiveKVMsg.getValue()==null) {
            		throw new KVException(ERROR_INVALID_FORMAT);
            	}else{
            		return receiveKVMsg.getValue();
            	}
            }else{
        	throw new KVException (receiveKVMsg.getMessage());
            }
        } finally {
            closeHost(sock);
        }
    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
        if (key == null || key.equals("")) {
            throw new KVException(ERROR_INVALID_KEY);
        }    
        KVMessage receiveKVMsg = sendMessage(DEL_REQ, key);

        if (receiveKVMsg.getMessage() == null) {
            throw new KVException(ERROR_INVALID_FORMAT);
        } else if (!receiveKVMsg.getMessage().equals(SUCCESS)){
            throw new KVException (receiveKVMsg.getMessage());
        }
    }

    /**
     * Send a generic message to a server.
     * Assume the message has already been checked for formatting elsewhere. 
     *
     * @param msgType to send to server. 
     * @param k1 First argument (usually key, context dependent)
     * @param k2 Second argument (usually value, context dependent)
     * @return Server's response.
     * @throws KVException if something goes horribly wrong. 
     */
    public KVMessage sendMessage(String msgType, String k1, String k2) 
            throws KVException {
        Socket sock = connectHost();
        KVMessage responseMessage = null;
        
        try {
            KVMessage sendKVMsg = new KVMessage(msgType);
            
            switch(msgType) {
                case PUT_REQ:
                    sendKVMsg.setValue(k2);
                case DEL_REQ:
                    sendKVMsg.setKey(k1);
            }
            sendKVMsg.sendMessage(sock);
            responseMessage = new KVMessage(sock, socketTimeout);
        } finally {
            closeHost(sock);
        } 
        return responseMessage;
    }
    /* No support for default arguments = TEH FAIL */
    public KVMessage sendMessage(String msgType) throws KVException {
        return sendMessage(msgType, null, null);
    }
    public KVMessage sendMessage(String msgType, String k1) throws KVException {
        return sendMessage(msgType, k1, null);
    }
}
