package jetbrains.exodus.distrubuted.server;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
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

import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

public class InitialReplicator {

    private static Logger log = LoggerFactory.getLogger(InitialReplicator.class);

    private final BlockingQueue<String> friendsQueue;
    private final Set<String> removedFriends;

    public InitialReplicator() {
        friendsQueue = new LinkedBlockingQueue<>();
        removedFriends = new HashSet<>();
        final Thread replicatingThread = new Thread(new ReplicationLoop());
        App.getInstance().addFriendsListener(new App.FriendsListener() {
            @Override
            public void friendAdded(@NotNull final String friend) {
                try {
                    friendsQueue.put(friend);
                } catch (InterruptedException e) {
                    log.error("Failed to put", e);
                }
            }

            @Override
            public void friendRemoved(@NotNull final String friend) {
                synchronized (removedFriends) {
                    removedFriends.add(friend);
                }
            }
        });
        replicatingThread.setDaemon(true);
        replicatingThread.setName("Distributed Exodus Initial Replicator");
        replicatingThread.start();
    }

    @SuppressWarnings("unchecked")
    private class ReplicationLoop implements Runnable {

        @Override
        public void run() {
            log.info("Initial Replicator started");
            try {
                final App app = App.getInstance();
                for (int i = 0; i < app.friendDegree; ++i) {
                    String friend;
                    do {
                        friend = friendsQueue.take();
                    } while (isFriendRemoved(friend));
                    log.info("Replication of [" + friend + "] started");
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
                        log.info("Nothing to replicate");
                    } else {
                        log.info("Getting data from [" + friend + "] more recent than " + DateFormat.getDateInstance(DateFormat.LONG).format(new Date(lastTimeStamp)));
                        final List<NameSpaceKVIterableTuple>[] data = new List[]{null};
                        final RemoteConnector conn = RemoteConnector.getInstance();
                        try {
                            data[0] = conn.data(friend, lastTimeStamp, 10000);
                        } catch (TimeoutException e) {
                            log.warn("Replication of [" + friend + "] timed out");
                        } catch (Throwable t) {
                            log.error("Replication of [" + friend + "] failed", t);
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
                    log.info("Replication of [" + friend + "] finished");
                }
            } catch (Throwable t) {
                log.error("Initial Replicator error", t);
            } finally {
                log.info("Initial Replicator finished");
            }
        }

        private boolean isFriendRemoved(@NotNull final String friend) {
            synchronized (removedFriends) {
                return removedFriends.contains(friend);
            }
        }
    }
}
