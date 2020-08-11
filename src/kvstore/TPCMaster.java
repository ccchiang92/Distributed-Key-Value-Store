package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TPCMaster {
    private class Lib extends AssertWrapper {}
    static boolean DEBUG = true;
    private int numSlaves;
    private KVCache masterCache;
    Object registrationBlock = new Object();
    Object keyBlock = new Object();
    String workingKey;
    TreeMap<Long, TPCSlaveInfo> registeredTreeMap;
    ReentrantLock slavesLock;

    public static final int TIMEOUT = 3000;

    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        // implement me
        this.registeredTreeMap = new TreeMap<Long, TPCSlaveInfo>(new unsignedComparator());
        slavesLock=new ReentrantLock();
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered.Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public void registerSlave(TPCSlaveInfo slave) {
        // implement me
    	if (registeredTreeMap.containsKey(slave.getSlaveID())){
    		slavesLock.lock();
			registeredTreeMap.put(slave.getSlaveID(), slave);
			slavesLock.unlock();    		
    	}else if (registeredTreeMap.size()>=numSlaves){
    			return; //dropping registration if already at slave capacity
    	//}else{ if (slave.getSlaveID()==null){
    	//		return; //there is an error somewhere
    	}else{
    			slavesLock.lock();
    			registeredTreeMap.put(slave.getSlaveID(), slave);
    			slavesLock.unlock();
    			synchronized(registrationBlock){
    				if (registeredTreeMap.size()==numSlaves){
    					registrationBlock.notifyAll();
    				}
    			}
    	}
    	//}

    }
    //A Helper method for testing
    //Helps to ensure we are keeping the right slaves stored
    public TreeMap<Long, TPCSlaveInfo> getMap(){
    	return registeredTreeMap;
    }

    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }
    
    
    private class unsignedComparator implements Comparator<Long> {
		public int compare (Long a, Long b) {
			if (isLessThanUnsigned(a, b)) {
				return -1;
			} else if (isLessThanEqualUnsigned(a, b)) {
				return 0;
			} else {
				return 1;
			}
		}

	}
    
    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
        // implement me
    	if (registeredTreeMap.isEmpty()) {
			return null;
    	}else {
			SortedMap<Long, TPCSlaveInfo> slavesAfterKey = 
                registeredTreeMap.tailMap(hashTo64bit(key));
			if (slavesAfterKey.isEmpty()) {
				TPCSlaveInfo slave = registeredTreeMap.get(registeredTreeMap.firstKey());
				return slave;
			}else{
				TPCSlaveInfo slave = registeredTreeMap.get(slavesAfterKey.firstKey());
				return slave;
			}
    	}
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
        // implement me
    	if (registeredTreeMap.isEmpty()) {
			return null;
    	}else {
			SortedMap<Long, TPCSlaveInfo> slavesAfterFirst = 
                registeredTreeMap.tailMap(firstReplica.getSlaveID(),false);
			if (slavesAfterFirst.isEmpty()) {
				TPCSlaveInfo slave = registeredTreeMap.get(registeredTreeMap.firstKey());
				return slave;
			}else{
				TPCSlaveInfo slave = registeredTreeMap.get(slavesAfterFirst.firstKey());
				return slave;
			}
    	}
    }

    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq)
            throws KVException {
        okayToService(msg);
        Lib.assertTrue(numSlaves == getNumRegisteredSlaves());
        synchronized(keyBlock) {
            workingKey = msg.getKey();
        }

        /* Phase 1: VOTE_REQ */
        TPCSlaveInfo firstReplica = findFirstReplica(msg.getKey());
        TPCSlaveInfo secondReplica = findSuccessor(firstReplica);
        KVClient firstClient = new KVClient(firstReplica.getHostname(), firstReplica.getPort(), TIMEOUT);
        KVClient secondClient = new KVClient(secondReplica.getHostname(), secondReplica.getPort(), TIMEOUT);
        KVMessage firstReply = new KVMessage(ABORT);
        KVMessage secondReply = new KVMessage(ABORT);
        try {
            firstReply = firstClient.sendMessage(msg.getMsgType(), msg.getKey(), msg.getValue());
            secondReply = secondClient.sendMessage(msg.getMsgType(), msg.getKey(), msg.getValue());
        } catch(KVException e) {
            firstReply = e.getKVMessage(); 
            secondReply = e.getKVMessage(); 
        }

        Lib.assertTrue(firstReplica.getSlaveID() != secondReplica.getSlaveID(),
            "ERROR: handleTPCRequest got two of the same slave.");

        /* Phase 2: GLOBAL_* */
        if(firstReply.getMsgType().equals(READY) && secondReply.getMsgType().equals(READY)) {
            sendDecision(msg.getKey(), COMMIT);
            cache(msg);
            synchronized(keyBlock) {
                workingKey = null;
                keyBlock.notify();
            }
        } else {
            sendDecision(msg.getKey(), ABORT);
            synchronized(keyBlock) {
                workingKey = null;
                keyBlock.notify();
            }
            throw new KVException(firstReply.getMessage());
        }
    }

    /**
     * Send a decision (i.e., COMMIT or ABORT) to a replica set. 
     * Block until an ACK is received from both slaves involved.
     *
     * @param key The Key we're updating in this 2PC. 
     * @param decision COMMIT or ABOR
     */
    private void sendDecision(String key, String decision) {
        KVClient firstClient, secondClient;
        TPCSlaveInfo firstReplica;
        boolean firstAck = false;
        boolean secondAck = false;

        do {
            firstReplica = findFirstReplica(key);
            firstClient = new KVClient(firstReplica.getHostname(), firstReplica.getPort(), TIMEOUT);
            try {
                firstAck = firstClient.sendMessage(decision).getMsgType().equals(ACK);
            } catch(Exception e) {}
        } while(!firstAck);

        do {
            TPCSlaveInfo secondReplica = findSuccessor(firstReplica);
            secondClient = new KVClient(secondReplica.getHostname(), secondReplica.getPort(), TIMEOUT);
            try {
                secondAck = secondClient.sendMessage(decision).getMsgType().equals(ACK);
            } catch(Exception e) {}
        } while(!secondAck);
    }

    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVExceptions from both replicas
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from either slave for any reason
     */
    public String handleGet(KVMessage msg) throws KVException {
        okayToService(msg);           
        Lib.assertTrue(msg.getKey() != workingKey, "ERROR: Key conflict");
        KVMessage responseMessage = null;
        String response = null;
        TPCSlaveInfo firstSlave = findFirstReplica(msg.getKey());

        if((response = masterCache.get(msg.getKey())) != null)
            return response;
        if((response = getFromSlave(msg, firstSlave)) != null)
            return response;
        if((response = getFromSlave(msg, findSuccessor(firstSlave))) != null)
            return response;
        throw new KVException(ERROR_NO_SUCH_KEY);
    }

    /** 
     * Check for data conflicts or too few slaves registered 
     */
    private  void okayToService(KVMessage msg) {
        synchronized(keyBlock) {
            if(msg.getKey().equals(workingKey) && workingKey != null) {
                try {
                    keyBlock.wait();
                } catch(InterruptedException e) {}
            }
        }
        synchronized(registrationBlock) {
            if(getNumRegisteredSlaves() < numSlaves) {
                try {
                    registrationBlock.wait();
                } catch(Exception e) {}
            }
        }
    }

    /**
     * Connect to a slave and return the value of get(msg) on that slave.
     * 
     * @param msg whose key will be used to query the slave. 
     * @param slave we're trying to get() from.
     * @return value associated with msg.getKey() or null if no key stored. 
     */
    protected String getFromSlave(KVMessage msg, TPCSlaveInfo slave)
            throws KVException {
        return (new KVClient(slave.getHostname(), slave.getPort()).get(msg.getKey()));
    }

    /** 
     * @return the number of registered slaves 
     */
    private int getNumRegisteredSlaves() {
        int result = 0;
        slavesLock.lock();
            result = registeredTreeMap.size();
        slavesLock.unlock();
        return result;
    }

    /**
     * Manipulate the masterCache in response to a successful COMMIT action.
     *
     * @param msg to be parsed for cache manipulation.
     */
     private void cache(KVMessage msg) {
        switch(msg.getMsgType()) {
            case PUT_REQ:
                masterCache.put(msg.getKey(), msg.getValue());
                break;
            case DEL_REQ:
                masterCache.del(msg.getKey());
                break;
        }
     }

    /** 
     * Wrapper to catch errors during development. 
     * This class' functionality can be disabled with the configuration variable:
     * DEBUG.
     */
    protected static class AssertWrapper {
        /**
         * Verify a boolean condition.
         *
         * @param condition to check.
         * @param message to return in case of failure.
         */
        protected static void assertTrue(boolean condition, String failMessage) {
            if(DEBUG) {
                assert condition : failMessage;
            }
        }
       
        protected static void assertTrue(boolean condition) {
            assertTrue(condition, "Sanity check failed");
        }

        /**
         * Assert the statement is never reached.
         */
        protected static void assertNotReached(String failMessage) {
            assertTrue(false, failMessage);
        }

        protected static void assertNotReached() {
            assertTrue(false);
        }

        /**
         * Write a debugging message to stdout
         */
        protected static void debug(String message) {
            if(DEBUG) {
                System.out.println("[ DEBUG ]: "+message);
            }
        }
    } // End of AssertWrapper class.
} // End of TPCMaster class. 
