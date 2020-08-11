package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;



/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * It uses a threadPool to ensure that none of it's methods are blocking.
 */
public class TPCClientHandler implements NetworkHandler {

    private TPCMaster tpcMaster;
    private ThreadPool threadPool;

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     */
    public TPCClientHandler(TPCMaster tpcMaster) {
        this(tpcMaster, 1);
    }

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCClientHandler(TPCMaster tpcMaster, int connections) {
        // implement me
    	threadPool = new ThreadPool(connections);
		this.tpcMaster = tpcMaster;
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        // implement me
    	try {
            threadPool.addJob(new ClientHandler(client));
        }
        catch(InterruptedException e) {
        }
    }

    /**
     * Runnable class containing routine to service a request from the client.
     */
    private class ClientHandler implements Runnable {

        private Socket client = null;

        /**
         * Construct a ClientHandler.
         *
         * @param client Socket connected to client with the request
         */
        public ClientHandler(Socket client) {
            this.client = client;
        }

        /**
         * Processes request from client and sends back a response with the
         * result. The delivery of the response is best-effort. If we are
         * unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            // implement me
        	 String returnValue = "";
             String returnKey = "";
             String messageType = "";
             String returnMessage = SUCCESS;
             boolean isError = false;
             
             try {
                 KVMessage receivedMessage = new KVMessage(client);
                 messageType = receivedMessage.getMsgType();

                 switch(messageType) {
                     case GET_REQ:  
                         returnValue = tpcMaster.handleGet(receivedMessage);
                         returnKey = receivedMessage.getKey(); 
                         break;
                     case PUT_REQ:
                    	 tpcMaster.handleTPCRequest(receivedMessage, true);
                         break;
                     case DEL_REQ:
                    	 tpcMaster.handleTPCRequest(receivedMessage, false);
                         break;
                     default: 
                         throw(new KVException(ERROR_INVALID_FORMAT));
                 }
             } 
             catch(KVException kvErr) {
                 returnMessage = kvErr.getKVMessage().getMessage();
                 isError = true;
             }

             try {
                 KVMessage responseMessage = new KVMessage(RESP);

                 if(messageType.equals(GET_REQ) && !isError) {
                     responseMessage.setValue(returnValue);
                     responseMessage.setKey(returnKey);
                 }
                 else 
                     responseMessage.setMessage(returnMessage);
                 responseMessage.sendMessage(client);
             } 
             catch(KVException kvErr) {
             } 
             finally {
                 try {
                     client.close();
                 } 
                 catch(IOException e) {
                 }
             }
        }
    }

}
