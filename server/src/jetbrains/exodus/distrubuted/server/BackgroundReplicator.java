package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.client.ClientHandlerException;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.database.ByteIterable;
import jetbrains.exodus.database.impl.bindings.LongBinding;
import jetbrains.exodus.database.impl.bindings.StringBinding;
import jetbrains.exodus.database.impl.iterate.ArrayByteIterable;
import jetbrains.exodus.database.persistence.Store;
import jetbrains.exodus.database.persistence.Transaction;
import jetbrains.exodus.database.persistence.TransactionalComputable;
import jetbrains.exodus.database.persistence.TransactionalExecutable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

public class BackgroundReplicator {

    private static Logger log = LoggerFactory.getLogger(BackgroundReplicator.class);
    private static String fakeFriend = "http://fake.friend/";

    private final BlockingQueue<String> friendsQueue;
    private final Thread replicatingThread;

    public BackgroundReplicator() {
        friendsQueue = new LinkedBlockingQueue<>();
        replicatingThread = new Thread(new ReplicationLoop());
        App.getInstance().addFriendsListener(new App.FriendsListener() {
            @Override
            public void friendAdded(@NotNull final String friend) {
                friendsQueue.offer(friend);
            }

            @Override
            public void friendRemoved(@NotNull final String friend) {
                // nothing to do
            }
        });
        replicatingThread.setDaemon(true);
        replicatingThread.setName("Distributed Exodus Background Replicator");
        replicatingThread.start();
    }

    public void close() {
        friendsQueue.offer(fakeFriend);
        try {
            replicatingThread.join();
        } catch (InterruptedException e) {
            log.error("Failed to join replication loop thread", e);
        }
    }

    private String takeFriend() {
        try {
            return friendsQueue.take();
        } catch (InterruptedException e) {
            return fakeFriend;
        }
    }

    @SuppressWarnings("unchecked")
    private class ReplicationLoop implements Runnable {

        @Override
        public void run() {
            log.info("Background Replicator started");
            final App app = App.getInstance();
            final URI baseURI = app.getBaseURI();
            final RemoteConnector conn = RemoteConnector.getInstance();

            String friend;
            //noinspection StringEquality
            while ((friend = takeFriend()) != fakeFriend) {
                try {
                    // at first replicate friends
                    if (!URI.create(friend).equals(baseURI)) {
                        log.info("Replicating friends of [" + friend + "] started");
                        try {
                            app.addFriends(conn.friends(friend, baseURI.toString(), 10000));
                        } catch (TimeoutException e) {
                            log.warn("Replicating friends of [" + friend + "] timed out");
                        }
                        log.info("Replicating friends of [" + friend + "] finished");
                    }

                    // then replicate data
                    log.info("Replicating data of [" + friend + "] started");
                    log.info("Computing timestamp bound");
                    final long lastTimeStamp = app.getEnvironment().computeInTransaction(new TransactionalComputable<Long>() {
                        @Override
                        public Long compute(@NotNull final Transaction txn) {
                            long result = 0;
                            for (final String ns : app.getNamespaces(txn)) {
                                if (result == 0) {
                                    result = Long.MAX_VALUE;
                                }
                                final ArrayByteIterable nsKey = StringBinding.stringToEntry(ns);
                                final ByteIterable timeStampEntry = app.getNamespacesIdx().get(txn, nsKey);
                                if (timeStampEntry != null) {
                                    final long nsTimestamp = LongBinding.compressedEntryToLong(timeStampEntry);
                                    if (nsTimestamp < result) {
                                        result = nsTimestamp;
                                    }
                                }
                            }
                            return result;
                        }
                    });
                    if (lastTimeStamp == Long.MAX_VALUE) {
                        log.info("No data to replicate from [" + friend + "]");
                    } else {
                        log.info("Getting data from [" + friend + "] more recent than " + DateFormat.getDateTimeInstance().format(new Date(lastTimeStamp)));
                        final List<NameSpaceKVIterableTuple>[] data = new List[]{null};
                        try {
                            data[0] = conn.data(friend, lastTimeStamp, 10000);
                        } catch (TimeoutException e) {
                            log.warn("Replicating data of [" + friend + "] timed out");
                        } catch (Throwable t) {
                            log.error("Replicating data of [" + friend + "] failed", t);
                        }
                        if (data[0] != null) {
                            log.info("Saving data of [" + friend + "]");
                            app.getEnvironment().executeInTransaction(new TransactionalExecutable() {
                                @Override
                                public void execute(@NotNull final Transaction txn) {
                                    for (final NameSpaceKVIterableTuple nsTuple : data[0]) {
                                        final String ns = nsTuple.getNamespace();
                                        final Pair<Store, Store> nsStores = app.getNsStores(ns);
                                        for (final KeyValueTuple dataTuple : nsTuple.getData()) {
                                            Database.putLocally(txn, nsStores.getFirst(), nsStores.getSecond(), app,
                                                    dataTuple.getKey(), dataTuple.getValue(), dataTuple.getTimeStamp());
                                        }
                                    }
                                }
                            });
                        }
                    }
                    log.info("Replicating data of [" + friend + "] finished");
                } catch (ClientHandlerException t) {
                    log.warn("Can't reach friend [" + friend + "]: " + t.getMessage());
                } catch (Throwable t) {
                    log.error("Background Replicator error, friend [" + friend + "]", t);
                }
            }
            log.info("Background Replicator finished");
        }
    }
}
