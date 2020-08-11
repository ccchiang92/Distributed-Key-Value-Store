package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class TPCRegistrationHandler implements NetworkHandler {

    private ThreadPool threadpool;
    private TPCMaster master;

    /**
     * Constructs a TPCRegistrationHandler with a ThreadPool of a single thread.
     *
     * @param master TPCMaster to register slave with
     */
    public TPCRegistrationHandler(TPCMaster master) {
        this(master, 1);
    }

    /**
     * Constructs a TPCRegistrationHandler with ThreadPool of thread equal to the
     * number given as connections.
     *
     * @param master TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCRegistrationHandler(TPCMaster master, int connections) {
        this.threadpool = new ThreadPool(connections);
        this.master = master;
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param slave Socket connected to the slave with the request
     */
    @Override
    public void handle(Socket slave) {
        try {
            threadpool.addJob(new RegistrationHandler(slave));
        } catch (InterruptedException e) { }
    }

    /**
     * Runnable class containing routine to service a registration request from
     * a slave.
     */
    public class RegistrationHandler implements Runnable {

        public Socket slave = null;

        public RegistrationHandler(Socket slave) {
            this.slave = slave;
        }

        /**
         * Parse registration request from slave and add register with TPCMaster.
         * If able to successfully parse request and register slave, send back
         * a successful response according to spec. If not, send back a response
         * with ERROR_INVALID_FORMAT.
         */
        @Override
        public void run() {
            try {
                KVMessage receivedMessage = new KVMessage(slave);
                String registrationString = receivedMessage.getMessage();
                TPCSlaveInfo slaveInfo = new TPCSlaveInfo(registrationString);
                master.registerSlave(slaveInfo);

                KVMessage response = new KVMessage(RESP, "Successfully registered " + registrationString);
                response.sendMessage(slave);
            } catch (KVException e) {
                if (e.getKVMessage().getMessage().equals(ERROR_INVALID_FORMAT)) {
                    KVMessage response = new KVMessage(RESP, ERROR_INVALID_FORMAT);
                    try {
                        response.sendMessage(slave);
                    } catch (KVException e2) {
                        // TODO what do I do here?
                    }
                } else {
                    // TODO what do I do here?
                }
            }
        }
    }
}
