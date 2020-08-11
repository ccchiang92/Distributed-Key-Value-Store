package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;

import org.junit.*;

import java.util.*;

public class KVStoreTest {

    KVStore store;

    @Before
    public void setupStore() {
        store = new KVStore();
    }

    @Test
    public void putAndGetOneKey() throws KVException {
        String key = "this is the key.";
        String val = "this is the value.";
        store.put(key, val);
        assertEquals(val, store.get(key));
    }

    @Test
    public void serializationAndDeserializationTest() throws KVException {
        // Fill a store with random data, dump to a file, then make another
        // store, restoring from that file. Check that the store has the
        // correct data.

        Random rand = new Random(42);
        Map<String, String> map = new HashMap<String, String>(10000);
        String key, val;
        KVStore originalStore = new KVStore();
        for (int i = 0; i < 10000; i++) {
            key = Integer.toString(rand.nextInt());
            val = Integer.toString(rand.nextInt());
            originalStore.put(key, val);
            map.put(key, val);
        }

        originalStore.dumpToFile("fuzz-store-data.xml");
        KVStore newStore = new KVStore();
        newStore.restoreFromFile("fuzz-store-data.xml");

        Iterator<Map.Entry<String, String>> mapIter = map.entrySet().iterator();
        Map.Entry<String, String> pair;
        while(mapIter.hasNext()) {
            pair = mapIter.next();
            assertEquals(true, storeHasKey(newStore, pair.getKey()));
            assertEquals(pair.getValue(), newStore.get(pair.getKey()));
            mapIter.remove();
        }
        assertTrue(map.size() == 0);
    }

    private boolean storeHasKey(KVStore store, String key) {
        try {
            store.get(key);
        } catch (KVException e) {
            assertEquals(ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
            return false;
        }
        return true;
    }

}
