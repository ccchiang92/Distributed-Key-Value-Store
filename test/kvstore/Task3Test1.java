package kvstore;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Chris Chiang
 * Test for TPCMaster Methods for Task3
 * The tests do not use other parts of the project and will work regardless of other part's correctness
 * Part of the test depends of a helper method that returns the treemap of slaves from TPCMaster
 */

public class Task3Test1 {

	//Some setup values borrowed from TPCEndToEndTemplate
	static final int CLIENTPORT = 8888;
	static final int SLAVEPORT = 9090;

	static final int NUMSLAVES = 4;

	static final long SLAVE1 = 4611686018427387903L;  // Long.MAX_VALUE/2
	static final long SLAVE2 = 9223372036854775807L;  // Long.MAX_VALUE
	static final long SLAVE3 = -4611686018427387903L; // Long.MIN_VALUE/2
	static final long SLAVE4 = -0000000000000000001;  // Long.MIN_VALUE

	static final String KEY1 = "6666666666666666666"; // 2846774474343087985
	static final String KEY2 = "9999999999999999999"; // 8204764838124603412
	static final String KEY3 = "0000000000000000000"; //-7869206253219942869
	static final String KEY4 = "3333333333333333333"; //-2511215889438427442

	TPCSlaveInfo slave1;
	TPCSlaveInfo slave2;
	TPCSlaveInfo slave3;
	TPCSlaveInfo slave4;
	long lessThanMin;
	TPCMaster master;

	//A copy of the unsignedcomparator's compare method in TPCMaster
	//Had a bug in the comparator, thus some tests are written for it here
	public int compare (Long a, Long b) {
		if (TPCMaster.isLessThanUnsigned(a, b)) {
			return -1;
		} else if (TPCMaster.isLessThanEqualUnsigned(a, b)) {
			return 0;
		} else {
			return 1;
		}
	}

	@Before
	public void setUp() throws Exception {
		//Creating slaveInfos
		slave1= new TPCSlaveInfo(String.valueOf(SLAVE1)+"@Hostname:"+"9090");
		slave2= new TPCSlaveInfo(String.valueOf(SLAVE2)+"@Hostname:"+"9090");
		slave3= new TPCSlaveInfo(String.valueOf(SLAVE3)+"@Hostname:"+"9090");
		lessThanMin = SLAVE3+10000;
		slave4= new TPCSlaveInfo(String.valueOf(lessThanMin)+"@Hostname:"+"9090");
		master = new TPCMaster(NUMSLAVES, new KVCache(1,4));
		master.registerSlave(slave1);
		master.registerSlave(slave2);
		master.registerSlave(slave3);
		master.registerSlave(slave4);

	}
	//Tests that the setup correctly sets up the master's slaves
	//and that the method TPCMaster.RegisterSlaves() is correctly putting the slaves in a treemap
	@Test(timeout = 30000)
	public void RegistrationSetupTest() throws KVException {
		//Make sure SlaveIDs are correct
		//Test the SlaveInfo Constructor
		assertEquals(slave1.getSlaveID(),SLAVE1);
		assertEquals(slave2.getSlaveID(),SLAVE2);
		assertEquals(slave3.getSlaveID(),SLAVE3);
		assertEquals(slave4.getSlaveID(),lessThanMin);

		//Check TailMap behavior and makeSure all keys are correct/in-order
		TreeMap map =master.getMap();
		assertEquals(map.firstKey(),SLAVE1);
		assertEquals(map.tailMap(SLAVE1,false).firstKey(),SLAVE2);
		assertEquals(map.tailMap(SLAVE2,false).firstKey(),SLAVE3);
		assertEquals(map.tailMap(SLAVE3,false).firstKey(),lessThanMin);
		assertEquals(map.tailMap(SLAVE4).size(),0);
		assertEquals(map.size(),4);
	}

	//Tests various Edge cases of TPCMaster.RegisterSlaves(), Such as re-registering and registering more than 4 slaves
	@Test(timeout = 30000)
	public void RegisterSlaveTest() throws KVException {

		//Tries to register more than 4 slaves
		long SLAVE5 = -0000000000000000002;
		TPCSlaveInfo slave5= new TPCSlaveInfo(String.valueOf(SLAVE5)+"@Hostname:"+"9090");
		master.registerSlave(slave5);
		long SLAVE7 = 3232;
		TPCSlaveInfo slave7= new TPCSlaveInfo(String.valueOf(SLAVE5)+"@Hostname:"+"9090");
		master.registerSlave(slave7);
		TreeMap map =master.getMap();
		assertTrue(TPCMaster.hashTo64bit(String.valueOf(SLAVE4))!=SLAVE4);//can not use ID as keys
		assertEquals(map.size(),4);//fifth slave does not register
		assertFalse(map.containsKey(SLAVE5));
		assertFalse(map.containsKey(SLAVE7));

		//Re-Registeration works
		TPCSlaveInfo slave6= new TPCSlaveInfo(String.valueOf(SLAVE1)+"@ABCD:"+"8888080");
		master.registerSlave(slave6);
		map =master.getMap();
		TPCSlaveInfo tempSlave= (TPCSlaveInfo) map.get(SLAVE1);
		assertEquals(tempSlave.getHostname(),"ABCD");
		assertEquals(tempSlave.getPort(),8888080);

	}
	//Test that FindFirstReplica and FindSuccesor works as intended
	@Test(timeout = 30000)
	public void ReplicaAndSuccesorTest() throws KVException {	
		//Test basic ordering of find first
		assertEquals(master.findFirstReplica(KEY1).getSlaveID(), SLAVE1);
		assertEquals(master.findFirstReplica(KEY2).getSlaveID(), SLAVE2);
		assertEquals(master.findFirstReplica(KEY3).getSlaveID(), SLAVE3);
		assertEquals(master.findFirstReplica(KEY4).getSlaveID(), SLAVE1);//Edge case where it loops back to slave1

		//Test basic ordering of find successor
		assertEquals(master.findSuccessor(slave1).getSlaveID(),SLAVE2);
		assertEquals(master.findSuccessor(slave2).getSlaveID(),SLAVE3);
		assertEquals(master.findSuccessor(slave3).getSlaveID(),lessThanMin);
		assertEquals(master.findSuccessor(slave4).getSlaveID(),SLAVE1);

		//Test edge case for wierd key inputs
		String KEY5 = "-6dsfdsfdsf66"; 
		String KEY6 = "oh Yeah"; 
		String KEY7 = "Blaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0"; 
		String KEY8 = "kkkkkkkkkkkkk";
		System.out.println(master.findFirstReplica(KEY5).getSlaveID());
		System.out.println(master.findFirstReplica(KEY6).getSlaveID());
		System.out.println(master.findFirstReplica(KEY7).getSlaveID());
		System.out.println(master.findFirstReplica(KEY8).getSlaveID());



	}

	//Make sure the treemap comparator is working as intended
	@Test(timeout = 30000)
	public void ComparatorTests() throws KVException {
		assertEquals(compare(SLAVE1,SLAVE1),0);
		assertEquals(compare(SLAVE1,SLAVE2),-1);
		assertEquals(compare(SLAVE2,SLAVE1),1);
		assertEquals(compare(SLAVE1,SLAVE4),-1);
		assertEquals(compare(SLAVE4,SLAVE1),1);

	}

}
