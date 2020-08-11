package kvstore;
import static org.mockito.Mockito.*;
import org.junit.*; 
import static org.junit.Assert.*;
import org.junit.Test;
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.util.concurrent.locks.*;
import java.net.InetAddress;
import java.io.*;
import java.io.FileReader;
public class TaskFiveTest extends EndToEndTemplate
{
    int NUM_THREADS = 5;
    static int data = 0;
    Object lockObject = new Object();
    ThreadPool testPool = new ThreadPool(NUM_THREADS);
    ThreadPool spyPool = spy(testPool);
    
    /* Class used for ThreadPool tests */
    private class ThreadRunner implements Runnable
    {
        public void run() {
            loopie();
            synchronized(lockObject) {
                data += 1;
                //System.out.println("Incrementing data, now "+data);
            }
            if(spyPool.getNumJobs() == 0)
                synchronized(spyPool) {
                    spyPool.notify();
                }
        }

        /** Run a loop for a long time */
        private boolean loopie() {
            int NUM_RUNS = 100000;
            for(int i =0; i<NUM_RUNS; i++) {}
            return true;
        }    
        
        /** Helper method for poolTest that will cause deadlock */
        private boolean dl(Lock deadLock) {
            deadLock.lock();
            return true;
        }
    }

    /** Implements a client application to a single server */
    private class ClientApplication implements Runnable
    {
        private int index;
        private KVClient client;

        public ClientApplication(int index) { 
            this.index = index;
            try {
            client = new KVClient(InetAddress.getLocalHost().getHostAddress(), 8080);
            } catch(Exception e) {}
        }

        public void run() {
            try {
            for(int i = index; i< index+1000; i++) 
                client.put((""+i), (""+(i+1)));
            } catch(KVException e) {}
        }
    }

    @Before
    public void init() {
    }

    /**
     * An extension of the testPutGet() method in EndToEndTest. 
     * Lets try to break the parser.
     */
    @Test(timeout = 20000)
    public void superPutGet() throws KVException {
        String exceptionCheck = "";

        client.put(".","%");
        client.put("*", "^");

        /* Send a goofy key*/
        try {
            client.put("\t\n*@<EOF>f f", "weirdo");
        } catch(KVException e) {
            fail("Should not have aborted here");
        }
        assertEquals("weirdo", client.get("\t\n*@<EOF>f f"));
        
        /* Send a null key put request */
        try {
            client.put("", "null");
        } catch (KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
        }
        assertEquals(KVConstants.ERROR_INVALID_KEY,  exceptionCheck);
        exceptionCheck = "";
         
        /* Now with a null value */
        try {
            client.put("null", "");
        } catch (KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
        }
        assertEquals(KVConstants.ERROR_INVALID_VALUE, exceptionCheck);
        exceptionCheck = "";

        assertEquals(client.get("*"), "^"); 
        assertEquals(client.get("."), "%"); 
    }

    /*
     * Make sure overflowing keys and values make it back to the client. 
     */
    @Test(timeout = 20000)
    public void overflowMe() {
        String exceptionCheck = "";
        String oversizedKey =
            "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"+ 
            "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"+ 
            "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"+ 
            "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"+ 
            "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"+ 
            "iiiiii"; // Length is 256
        String oversizedValue = "";
        String fileLine = "";

        /* 
         * Read a line of length (256*1024  1) from a file. 
         * FYI, VIM reallly doesn't appreciate having to make
         * such a line....next time, use Python.
         */
        try {
            String home = System.getProperty("user.home");
            String filePath = home+"/group41-kvstore/kvstore/test/kvstore/oversizedValue.txt";
            BufferedReader fileReader = new BufferedReader(
                new FileReader(filePath));
            while((fileLine = fileReader.readLine()) != null) 
                oversizedValue += fileLine;
            fileReader.close();
        } catch (IOException e) {
        }

        /* This should NOT overflow the key value on KVServer */
        try {
            client.put(oversizedKey, "overflow");
            exceptionCheck = "okay";
        } catch (KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
            fail();
        }
        assertEquals("okay", exceptionCheck);
        exceptionCheck = "";

        /* This SHOULD overflow the key value on KVServer */
        try {
            client.put(oversizedKey+"i", "overflow");
        } catch (KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
        }
        assertEquals(KVConstants.ERROR_OVERSIZED_KEY, exceptionCheck);
        exceptionCheck = "";

        /* Value length 256*1024 - 1. Should NOT cause an error */ 
        try {
            client.put("oversizedvalue", oversizedValue);
            exceptionCheck = "okay";
        } catch(KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
            fail();
        }
        assertEquals("okay", exceptionCheck);
        exceptionCheck = "";

        /* Value length 256*1024. This SHOULD overflow the value on KVServer */
        try {
            client.put("oversizedvalue", oversizedValue+"i");
        } catch(KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
        }
        assertEquals(KVConstants.ERROR_OVERSIZED_VALUE, exceptionCheck);
        exceptionCheck = "";

        /* Now try the same thing but get() and del() on key */
        try {
            client.get("");
        } catch(KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
        }
        assertEquals(KVConstants.ERROR_INVALID_KEY, exceptionCheck);
        exceptionCheck = "";

        try {
            client.del("");
        } catch(KVException e) {
            exceptionCheck = e.getKVMessage().getMessage();
        }
        assertEquals(KVConstants.ERROR_INVALID_KEY, exceptionCheck);
        exceptionCheck = "";

    }

    /** Test the ThreadPool for errors */ 
    @Test(timeout=10000)
    public void poolTest() {
        /* Thread that will throw an exception */
        Runnable task15 = mock(Runnable.class);
        doThrow(new RuntimeException()).when(task15).run();

        /* ThreadRunners */
        ThreadRunner task1 = new ThreadRunner();
        ThreadRunner task2 = new ThreadRunner();
        ThreadRunner task3 = new ThreadRunner();
        ThreadRunner task7 = new ThreadRunner(); 
        ThreadRunner task8 = new ThreadRunner(); 
        ThreadRunner task9 = new ThreadRunner(); 
        ThreadRunner task11= new ThreadRunner(); 
        ThreadRunner task12= new ThreadRunner(); 
        ThreadRunner task13= new ThreadRunner(); 
        ThreadRunner task14= new ThreadRunner();
        ThreadRunner task4 = new ThreadRunner(); 
        ThreadRunner task5 = new ThreadRunner();
        ThreadRunner task6 = new ThreadRunner(); 
        ThreadRunner task10= new ThreadRunner(); 

        /* Spy objects */
        ThreadRunner taskSpy1 = spy(task1);
        ThreadRunner taskSpy2 = spy(task2);
        ThreadRunner taskSpy3 = spy(task3);
        ThreadRunner taskSpy4 = spy(task4);
        ThreadRunner taskSpy5 = spy(task5);
        ThreadRunner taskSpy6 = spy(task6);
        ThreadRunner taskSpy7 = spy(task7);
        ThreadRunner taskSpy8 = spy(task8);
        ThreadRunner taskSpy9 = spy(task9);
        ThreadRunner taskSpy10 = spy(task10);
        ThreadRunner taskSpy11 = spy(task11);
        ThreadRunner taskSpy12 = spy(task12);
        ThreadRunner taskSpy13 = spy(task13);
        ThreadRunner taskSpy14 = spy(task14);

        /* Add these tasks to the ThreadPool */
        try {
            spyPool.addJob(taskSpy1);
            spyPool.addJob(taskSpy2);
            spyPool.addJob(taskSpy3);
            spyPool.addJob(taskSpy4);
            spyPool.addJob(task15);
            spyPool.addJob(taskSpy5);
            spyPool.addJob(taskSpy6);
        } catch (InterruptedException e) {}

        /* Add some more jobs later */
        try {
            spyPool.addJob(taskSpy8);
            spyPool.addJob(taskSpy7);
            spyPool.addJob(taskSpy9);
            spyPool.addJob(taskSpy14);
            spyPool.addJob(taskSpy10);
            spyPool.addJob(taskSpy13);
            spyPool.addJob(taskSpy11);
            spyPool.addJob(taskSpy12);
        } catch (InterruptedException e) {}

        try {
            synchronized(spyPool) {
                spyPool.wait();
            }
        } catch(InterruptedException e) {}
                
        /* Verify that all tasks were run() */
        while(data != 14)
            Thread.yield();
        verify(taskSpy1).run();
        verify(taskSpy2).run();
        verify(taskSpy3).run(); 
        verify(taskSpy4).run();
        verify(taskSpy5).run();
        verify(taskSpy6).run();
        verify(taskSpy7).run();
        verify(taskSpy8).run();
        verify(taskSpy9).run();
        verify(taskSpy10).run();
        verify(taskSpy11).run();
        verify(taskSpy12).run();
        verify(taskSpy13).run();
        verify(taskSpy14).run();
        verify(task15).run();

        /* Verify Verify getJob was called at least 2*NUM_THREADS times */
        try {
            verify(spyPool, times(15)).addJob((Runnable)anyObject());    
        } catch(InterruptedException e) {}

        /* Make sure no threads died while running */
        assertEquals(NUM_THREADS, spyPool.getNumLivingThreads());
        assertEquals(14, data);
    } 

    /** Run multiple clients against a single server */
    @Test(timeout=25000)
    public void multiRole() {
        ClientApplication client1 = new ClientApplication(0);
        ClientApplication client2 = new ClientApplication(1000);
        ClientApplication client3 = new ClientApplication(2000);
        ClientApplication client4 = new ClientApplication(3000);
        ClientApplication client5 = new ClientApplication(4000);

        Thread clientThread1 = new Thread(client1); 
        Thread clientThread2 = new Thread(client2); 
        Thread clientThread3 = new Thread(client3); 
        Thread clientThread4 = new Thread(client4); 
        Thread clientThread5 = new Thread(client5); 

        clientThread1.start();
        clientThread2.start();
        clientThread3.start();
        clientThread4.start();
        clientThread5.start();

        try {
            clientThread1.join();
            clientThread2.join();
            clientThread3.join();
            clientThread4.join();
            clientThread5.join();
        } catch(InterruptedException e) {}

        /* Now get the results */
        try {
            int correctValue = 0;
            for(int i = 0; i<= 5000; i++)
                correctValue += i;

            KVClient resultClient = new KVClient(InetAddress.getLocalHost().getHostAddress(), 8080);
            int result = 0;
            Integer resInt = null; 
            for(int i =0; i<5000; i++) {
                resInt = Integer.decode(resultClient.get((""+i)));
                result += resInt.intValue();
            }
            assertEquals(correctValue, result);
        } catch(Exception e) {}
    }
}
