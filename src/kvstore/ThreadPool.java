package kvstore;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPool 
{
    private Thread threads[];
    private ArrayList<Runnable> jobQueue; 
    private ReentrantLock jobQueueLock;
    private Object sleepObject;

    /**
     * Constructs a Threadpool with a starting number of threads.
     *
     * @param size number of threads in the thread pool
     */
    public ThreadPool(int size) 
    {
        threads = new Thread[size];
        jobQueue = new ArrayList<Runnable>();
        jobQueueLock = new ReentrantLock();
        sleepObject = new Object();
       
        jobQueueLock.lock();
            for(int i = 0; i<size; i++)  {
                threads[i] = new WorkerThread(this); 
                threads[i].start();
            }
        jobQueueLock.unlock();
    }

    /**
     * Add a job to the queue of jobs that have to be executed. As soon as a
     * thread is free, the thread will retrieve a job from this queue if
     * if one exists and start processing it.
     *
     * @param r job that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    public void addJob(Runnable r) throws InterruptedException 
    {
        jobQueueLock.lock();
            jobQueue.add(r);
            synchronized(sleepObject) {
                sleepObject.notify();
            }
        jobQueueLock.unlock();
    }

    /**
     * Block until a job is present in the queue and retrieve the job
     * @return A runnable task that has to be executed
     * @throws InterruptedException if thread is interrupted while in blocked
     *         state. Your implementation may or may not actually throw this.
     */
    public Runnable getJob() throws InterruptedException  
    {
        Runnable nextTask = null;
        jobQueueLock.lock();
            while(!(jobQueue.size() > 0)) {
                synchronized(sleepObject) {
                    jobQueueLock.unlock();
                    sleepObject.wait();
                }
                jobQueueLock.lock();
            }
        nextTask = jobQueue.remove(0);
        jobQueueLock.unlock();
        return nextTask;
    }

    /** Return the number of WorkerThreads still alive. Used for debug */
    protected int getNumLivingThreads() 
    {
        int aliveCount = 0;
        for(int i =0; i<threads.length; i++)
            if(((WorkerThread)threads[i]).alive)
                aliveCount++;
        return aliveCount;
    }

    /** Return the number of jobs currently in the queue. Used for debug */
    protected int getNumJobs() 
    {
        return jobQueue.size();
    }

    /** A thread in the thread pool. */
    protected class WorkerThread extends Thread 
    {
        private ThreadPool threadPool;
        protected boolean alive;
        public final int threadId = hashCode() % 1000;

        /**
         * Constructs a thread for a particular ThreadPool.
         *
         * @param pool the ThreadPool containing this thread
         */
        public WorkerThread(ThreadPool pool) 
        {
            threadPool = pool;
            alive = true;
        }

        /** Scan for and process tasks. */
        @Override
        public void run() 
        {
            try {
                while(alive) {
                    try {
                        threadPool.getJob().run();
                    } 
                    catch(Exception e) {
                    }
                } 
            } 
            catch(Exception e) {
            } 
            finally {
                alive = false;
            }
        }
    } // End WorkerThread class
} // End ThreadPool class
