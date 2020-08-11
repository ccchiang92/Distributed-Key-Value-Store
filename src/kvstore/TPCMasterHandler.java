package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    private long slaveID;
    private KVServer kvServer;
    private TPCLog tpcLog;
    private ThreadPool threadpool;

    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
        // TODO Do I need to connect() the server first, to make sure the port
        // is nonzero? What if it's already connect()ed?
        String slaveHostname = server.getHostname();
        int slavePort = server.getPort();
        String registrationMessage = slaveID + "@" + slaveHostname + ":" + slavePort;
        Socket sock;
        try {
            sock = new Socket(masterHostname, 9090);
        } catch(UnknownHostException e) {
            throw new KVException(ERROR_COULD_NOT_CONNECT); // TODO do i do these things?
        } catch(IOException f) {
            throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET); // TODO do i do these things?
        }
        KVMessage registrationMsg = new KVMessage(REGISTER, registrationMessage);
        registrationMsg.sendMessage(sock);
        KVMessage responseMsg = new KVMessage(sock); // this will throw an
                         // ERROR_INVALID_FORMAT KVException if bad format
        try {
            sock.close();
        } catch (IOException e) { }
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        // implement me
        
        // Create a job and add to the thread pool
        MasterHandler job = new MasterHandler(master);
        try {
            threadpool.addJob(job);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Runnable class containing routine to service a message from the master.
     */
    private class MasterHandler implements Runnable {

        private Socket master;

        /**
         * Construct a MasterHandler.
         *
         * @param master Socket connected to master with the message
         */
        public MasterHandler(Socket master) {
            this.master = master;
        }

        /**
         * Processes request from master and sends back a response with the
         * result. This method needs to handle both phase1 and phase2 messages
         * from the master. The delivery of the response is best-effort. If
         * we are unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            // implement me
            try {
                KVMessage kvm = new KVMessage(master);
                String msgTyp = kvm.getMsgType();
                switch(msgTyp) {
                    case PUT_REQ:
                        tpcLog.appendAndFlush(kvm);
                        handlePutReq(kvm.getKey(), kvm.getValue());
                        break;
                    case DEL_REQ:
                        tpcLog.appendAndFlush(kvm);
                        handleDelReq(kvm.getKey());
                        break;
                    case GET_REQ:
                        handleGetReq(kvm.getKey());
                        break;
                    case COMMIT:
                        handleCommit(kvm);
                        break;
                    case ABORT:
                        tpcLog.appendAndFlush(kvm);
                        handleAbort();
                        break;
                }
            } catch (KVException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        

        private void handlePutReq(String key, String value) {
            /* Check if key, value pair possible if not send abort
             * and error message else send ready vote 
             */
            KVMessage kvm;
            if (key.length() > KVServer.MAX_KEY_SIZE) {
                kvm = new KVMessage(ABORT, ERROR_OVERSIZED_KEY);
            } else if (value.length() > KVServer.MAX_VAL_SIZE) {
                kvm = new KVMessage(ABORT, ERROR_OVERSIZED_VALUE);
            }
            kvm = new KVMessage(READY);

            // Send the message to master
            try {
                kvm.sendMessage(master);
            } catch (KVException e) {
            }
        }

        private void handleDelReq(String key) {
            // Check if key exists if not send abort and error else send ready vote
            KVMessage kvm;
            if (kvServer.hasKey(key)) {
                kvm = new KVMessage(READY);
            } else {
                kvm = new KVMessage(ABORT, ERROR_NO_SUCH_KEY);
            }
            
            // Send the message to master
            try {
                kvm.sendMessage(master);
            } catch (KVException e) {
            }
            
        }
        
        private void handleGetReq(String key) {
            // Grab the value and construct the message
            KVMessage kvm;
            try {
                String value = kvServer.get(key);
                kvm = new KVMessage(RESP);
                kvm.setKey(key);
                kvm.setValue(value);
            } catch (KVException e) {
                kvm = e.getKVMessage();
            }
            
            // Send the message to master
            try {
                kvm.sendMessage(master);
            } catch (KVException e) {
            }
        }
        
        private void handleCommit(KVMessage currentKvm) {
            // Grab the last entry
            KVMessage lastKvm = tpcLog.getLastEntry();
            if (lastKvm.getMsgType().equals(PUT_REQ)) {
            	tpcLog.appendAndFlush(currentKvm);
                String key = lastKvm.getKey();
                String value = lastKvm.getValue();
                try {
                    kvServer.put(key, value);
                } catch (KVException e) {
                }
            } else if (lastKvm.getMsgType().equals(DEL_REQ)) {
            	tpcLog.appendAndFlush(currentKvm);
                String key = lastKvm.getKey();
                try {
                    kvServer.del(key);
                } catch (KVException e) {
                }
            }

            // Send ack to master
            KVMessage kvm = new KVMessage(ACK);
            try {
                kvm.sendMessage(master);
            } catch (KVException e) {
            }
        }
        
        private void handleAbort() {
            KVMessage kvm = new KVMessage(ACK);
            try {
                kvm.sendMessage(master);
            } catch (KVException e) {
            }
        }
    }



}
