package kvstore;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

public class KVServerTest {

    KVServer server;

    @Before
    public void setupServer() {
        server = new KVServer(10, 10);
    }

    @Test
    public void fuzzTest() throws KVException {
        Random rand = new Random(8); // no reason for 8
        Map<String, String> map = new HashMap<String, String>(10000);
        String key, val;
        for (int i = 0; i < 10000; i++) {
            key = Integer.toString(rand.nextInt());
            val = Integer.toString(rand.nextInt());
            server.put(key, val);
            map.put(key, val);
        }
        Iterator<Map.Entry<String, String>> mapIter = map.entrySet().iterator();
        Map.Entry<String, String> pair;
        while(mapIter.hasNext()) {
            pair = mapIter.next();
            assertTrue(server.hasKey(pair.getKey()));
            assertEquals(pair.getValue(), server.get(pair.getKey()));
            mapIter.remove();
        }
        assertTrue(map.size() == 0);
    }

    @Test
    public void testNonexistentGetFails() {
        getNonexistent("this key shouldn't be here");
    }

    @Test
    public void testNonexistentDelFails() {
        delNonexistent("this key shouldn't be here");
    }

    @Test
    public void testDel() throws KVException {
        final String key = "a key";
        server.put(key, "value doesn't matter");
        server.del(key);
        assertFalse(server.hasKey(key));
        getNonexistent(key);
        delNonexistent(key);
    }

    // getNonexistent and delNonexistent should be called with nonexistent
    // keys. They check that their respective methods throw the appropriate
    // exception.
    public void getNonexistent(String key) {
        try {
            server.get(key);
            fail("get with nonexistent key should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
        }
    }

    public void delNonexistent(String key) {
        try {
            server.del(key);
            fail("del with nonexistent key should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
        }
    }

    @Test
    public void tooLongKeyFails() {
        try {
            server.put(stringOfLength(257), "value does not matter");
        } catch (KVException e) {
            assertEquals(KVConstants.ERROR_OVERSIZED_KEY, e.getKVMessage().getMessage());
            return;
        }
        fail("oversized key did not cause exception");
    }

    @Test
    public void justShortEnoughKeySucceeds() throws KVException {
        server.put(stringOfLength(256), "value does not matter");
    }

    @Test
    public void tooLongValueFails() {
        try {
            server.put("key does not matter", stringOfLength(256*1024 + 1));
        } catch (KVException e) {
            assertEquals(KVConstants.ERROR_OVERSIZED_VALUE, e.getKVMessage().getMessage());
            return;
        }
        fail("oversized value did not cause exception");
    }

    @Test
    public void justShortEnoughValueSucceeds() throws KVException {
        server.put("key does not matter", stringOfLength(256*1024));
    }

    private static String stringOfLength(int n) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < n; i++) {
            result.append(".");
        }
        return result.toString();
    }


}
