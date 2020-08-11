package kvstore;

import static kvstore.KVConstants.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Client API used to send requests to key-value server.
 */
public class KVClient2 implements KeyValueInterface {

    private String server;
    private int port;
    private Socket Mocket;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port on which the server is listening
     */
    public KVClient2(String server, int port) {
        this.server = server;
        this.port = port;
    }

    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to create or connect socket
     */
    private Socket connectHost() throws KVException {
        // implement me
       
        	Socket mockSock=mock(Socket.class);
        	Mocket=mockSock;
            return mockSock;
        
    }
    public Socket getMock(){
    	return Mocket;
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
        // implement me
    	if (key == null||key.equals("")) {
            throw new KVException(ERROR_INVALID_KEY);
        } else if (value == null||value.equals("")) {
            throw new KVException(ERROR_INVALID_VALUE);
        }
        Socket sock = connectHost();
        KVMessage sendKVMsg = new KVMessage(PUT_REQ);
        sendKVMsg.setKey(key);
        sendKVMsg.setValue(value);
        sendKVMsg.sendMessage(sock);
        KVMessage receiveKVMsg = new KVMessage(sock);
        if (!receiveKVMsg.getMessage().equals(SUCCESS)){
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
        // implement me
        if (key == null||key.equals("")) {
            throw new KVException(ERROR_INVALID_KEY);
        }
        Socket sock = connectHost();
        KVMessage sendKVMsg = new KVMessage(GET_REQ);
        sendKVMsg.setKey(key);
        sendKVMsg.sendMessage(sock);
        KVMessage receiveKVMsg = new KVMessage(sock);
        if (receiveKVMsg.getMessage()!=null){
            throw new KVException (receiveKVMsg.getMessage());
        }
        if (!receiveKVMsg.getKey().equals(key)){
            throw new KVException ("Key Doesn't Match");   
        }
       
        return receiveKVMsg.getValue();
    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
        // implement me
    	if (key == null||key.equals("")) {
            throw new KVException(ERROR_INVALID_KEY);
        }
        Socket sock = connectHost();
        KVMessage sendKVMsg = new KVMessage(DEL_REQ);
        sendKVMsg.setKey(key);
        sendKVMsg.sendMessage(sock);
        KVMessage receiveKVMsg = new KVMessage(sock);
        if (!receiveKVMsg.getMessage().equals(SUCCESS)){
            throw new KVException (receiveKVMsg.getMessage());
        }
        
    }

}
