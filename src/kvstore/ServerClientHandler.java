package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.io.*;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {
    private KVServer kvServer;
    private ThreadPool threadPool;

    /**
     * Constructs a ServerClientHandler with ThreadPool of a single thread.
     *
     * @param kvServer KVServer to carry out requests
     */
    public ServerClientHandler(KVServer kvServer) {    
        this(kvServer, 1);
    }

    /**
     * Constructs a ServerClientHandler with ThreadPool of thread equal to
     * the number given as connections.
     *
     * @param kvServer KVServer to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public ServerClientHandler(KVServer kvServer, int connections) {
        threadPool = new ThreadPool(connections);
        this.kvServer = kvServer; 
    }

    /**
     * Creates a job to service the request for a socket and enqueues that job
     * in the thread pool. Ignore all InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
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
        private Socket client;

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
                        returnValue = kvServer.get(receivedMessage.getKey());
                        returnKey = receivedMessage.getKey(); 
                        break;
                    case PUT_REQ:
                        kvServer.put(receivedMessage.getKey(),receivedMessage.getValue());
                        break;
                    case DEL_REQ:
                        kvServer.del(receivedMessage.getKey());
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
   } // End of ClientHandler class
} // End of ServerClientHandler class
