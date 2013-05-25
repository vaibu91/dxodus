package jetbrains.exodus.distrubuted.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AsyncRunnableExecutor {

    private static Logger log = LoggerFactory.getLogger(AsyncRunnableExecutor.class);
    private static Runnable fakeAsync = new Runnable() {
        @Override
        public void run() {
        }
    };
    private final BlockingQueue<Runnable> asyncQueue;
    private final Thread replicatingThread;

    public AsyncRunnableExecutor() {
        asyncQueue = new LinkedBlockingQueue<>();
        replicatingThread = new Thread(new MainLoop());
        replicatingThread.setDaemon(true);
        replicatingThread.setName("async");
        replicatingThread.start();
    }

    public void execute(Runnable r) {
        asyncQueue.offer(r);
    }

    public void close() {
        asyncQueue.offer(fakeAsync);
        try {
            replicatingThread.join();
        } catch (InterruptedException e) {
            log.error("Failed to join replication loop thread", e);
        }
    }

    private Runnable takeAsync() {
        try {
            return asyncQueue.take();
        } catch (InterruptedException e) {
            return fakeAsync;
        }
    }

    private class MainLoop implements Runnable {
        @Override
        public void run() {
            log.info("Background async executor started");

            Runnable async;
            while ((async = takeAsync()) != fakeAsync) {
                try {
                    async.run();
                } catch (Throwable t) {
                    log.error("Async task failed", t);
                }
            }

            log.info("Background async executor finished");
        }
    }

}
