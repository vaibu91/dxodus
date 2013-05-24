package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.client.ClientResponse;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.database.ByteIterable;
import jetbrains.exodus.database.ByteIterator;
import jetbrains.exodus.database.impl.bindings.LongBinding;
import jetbrains.exodus.database.impl.bindings.StringBinding;
import jetbrains.exodus.database.impl.iterate.ArrayByteIterable;
import jetbrains.exodus.database.impl.iterate.IterableUtils;
import jetbrains.exodus.database.impl.iterate.LightOutputStream;
import jetbrains.exodus.database.persistence.Cursor;
import jetbrains.exodus.database.persistence.Store;
import jetbrains.exodus.database.persistence.Transaction;
import jetbrains.exodus.database.persistence.TransactionalComputable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Path("")
@Produces(MediaType.APPLICATION_JSON)
public class Database {

    private static Logger log = LoggerFactory.getLogger(Database.class);

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_PLAIN)
    public String ping() {
        return "dexodus";
    }

    @GET
    @Path("/html/{ns}/{key}")
    @Produces(MediaType.TEXT_HTML)
    public String doGetHtml(@PathParam("ns") final String ns, @PathParam("key") final String key) {
        return doGet(ns, key);
    }

    @GET
    @Path("/{ns}/{key}")
    @Produces(MediaType.TEXT_PLAIN)
    public String doGet(@PathParam("ns") final String ns, @PathParam("key") final String key) {
        log.info("GET: " + key);
        final ArrayByteIterable keyBytes = StringBinding.stringToEntry(key);
        final App app = App.getInstance();
        final String[] friends = app.getFriends();
        app.shuffle(friends);
        if (friends.length == 0) {
            return App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<String>() {
                @Override
                public String compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull Store idx, @NotNull App app) {
                    final ByteIterable valueBytes = namespace.get(txn, keyBytes);
                    if (valueBytes == null) {
                        log.info("No friends and no data by key: " + key);
                        throw new WebApplicationException(Response.Status.NOT_FOUND);
                    }
                    final ByteIterator itr = valueBytes.iterator();
                    itr.skip(8); // ignore timestamp
                    return IterableUtils.readString(itr);
                }
            });
        }
        final Map<Future, String> futureToFriends = new HashMap<>();
        final ValueTimeStampTuple seed = App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<ValueTimeStampTuple>() {
            @Override
            public ValueTimeStampTuple compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull Store idx, @NotNull App app) {
                final ByteIterable valueBytes = namespace.get(txn, keyBytes);
                if (valueBytes == null) {
                    return null;
                }
                final ByteIterator itr = valueBytes.iterator();
                final long t = IterableUtils.readLong(itr);
                return new ValueTimeStampTuple(t, IterableUtils.readString(itr));
            }
        });
        if (seed == null) {
            log.warn("No local data by key: " + key);
        }
        final long localTimeStamp = seed == null ? 0 : seed.getTimeStamp();
        final int maxFriends = Math.min(app.replicationReadDegree, friends.length);
        AsyncQuorum.Context<ValueTimeStampTuple, ValueTimeStampTuple> ctx =
                AsyncQuorum.createContext(Math.min(app.friendDegree, maxFriends),
                        new AsyncQuorum.ResultFilter<ValueTimeStampTuple, ValueTimeStampTuple>() {
                            @Nullable
                            @Override
                            public ValueTimeStampTuple fold(@Nullable ValueTimeStampTuple prev, @Nullable ValueTimeStampTuple current) {
                                if (current != null) {
                                    if (prev == null) {
                                        if (seed == null) {
                                            return current;
                                        }
                                        prev = seed;
                                    }
                                    if (current.getTimeStamp() > prev.getTimeStamp()) {
                                        return current;
                                    }
                                } else {
                                    log.error("Ignoring result " + current);
                                }
                                return prev;
                            }
                        }, new AsyncQuorum.ErrorHandler<ValueTimeStampTuple>() {
                            @Override
                            public void handleFailed(Future<ValueTimeStampTuple> failed, ExecutionException t) {
                                final String friend = futureToFriends.get(failed);
                                if (t == null) { // null means "cancelled"
                                    log.info("Get REPL cancelled for [" + friend + "]");
                                } else {
                                    log.warn("Removing [" + friend + "] due to exception " + t.getClass().getName() + ":" + t.getMessage());
                                    if (friend != null) { // workaround
                                        app.removeFriends(friend);
                                    }
                                }
                            }
                        },
                        RemoteConnector.REPL_TYPE
                );
        final Future[] futures = new Future[maxFriends];
        int i = 0;
        for (final String friend : friends) {
            log.info("Replicate get to: " + friend);
            final Future<ValueTimeStampTuple> future = RemoteConnector.getInstance().getAsyncRepl(friend, ns, key, localTimeStamp, ctx.getListener());
            futures[i++] = future;
            futureToFriends.put(future, friend);
            if (i >= maxFriends) {
                break;
            }
        }
        ctx.setFutures(futures);
        try {
            final ValueTimeStampTuple result = ctx.get(1000, TimeUnit.MILLISECONDS);
            ctx.cancel(true); // cancel other jobs
            log.info("Get replicated successfully");
            if (result == null) {
                log.info("No data by key at all: " + key);
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            }
            return result.getValue();
        } catch (QuorumException q) {
            log.warn("No quorum reached: " + key);
            if (seed != null) {
                return seed.getValue();
            }
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @GET
    @Path("/{ns}/{key}/{timeStamp}")
    public ValueTimeStampTuple doGetRepl(@PathParam("ns") final String ns, @PathParam("key") final String key,
                                         @PathParam("timeStamp") final Long timeStamp) {
        log.info("GET for replication: " + key);
        final ArrayByteIterable keyBytes = StringBinding.stringToEntry(key);
        final ByteIterable valueBytes = App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<ByteIterable>() {
            @Override
            public ByteIterable compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull Store idx, @NotNull App app) {
                return namespace.get(txn, keyBytes);
            }
        });
        if (valueBytes == null) {
            log.info("No timestamp for key " + key + ", requested " + timeStamp);
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        final ByteIterator itr = valueBytes.iterator();
        final long localTimeStamp = IterableUtils.readLong(itr);
        if (localTimeStamp < timeStamp) {
            log.info("Requested timestamp for key " + key + ": " + timeStamp + ", found: " + localTimeStamp);
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return new ValueTimeStampTuple(localTimeStamp, IterableUtils.readString(itr));
    }

    @POST
    @Path("/{ns}/{key}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public Response doPost(@PathParam("ns") final String ns, @PathParam("key") final String key,
                           @FormParam("value") final String value, @QueryParam("timeStamp") final Long timeStamp,
                           @Context UriInfo uriInfo) {
        log.info("POST to " + App.getInstance().getBaseURI().toString());

        final Long nextTimeStamp = putLocally(ns, key, value, timeStamp);

        if (nextTimeStamp == null) {
            log.info("Ignore put - timestamp is smaller.");
            return Response.status(Response.Status.NOT_ACCEPTABLE).build();
        }

        // replicate to friends
        replicateDoPost(ns, key, value, nextTimeStamp);

        return Response.ok().build();
    }

    @GET
    @Path("/friends")
    public String[] doGetFriends(@QueryParam("friendUri") final String friendUri) {
        final App app = App.getInstance();
        final String[] result = app.getFriends();
        if (friendUri != null) {
            app.addFriends(friendUri);
        }
        return result;
    }

    @GET
    @Path("/data/{timeStamp}")
    public List<NameSpaceKVIterableTuple> doGetData(@PathParam("timeStamp") final long timeStamp) {
        final App app = App.getInstance();
        return app.getEnvironment().computeInTransaction(new TransactionalComputable<List<NameSpaceKVIterableTuple>>() {
            @Override
            public List<NameSpaceKVIterableTuple> compute(@NotNull final Transaction txn) {
                final List<NameSpaceKVIterableTuple> result = new ArrayList<>();
                for (final String ns : app.getNamespaces(timeStamp, txn)) {
                    final Pair<Store, Store> stores = app.getNsStores(ns);
                    final Store namespace = stores.getFirst();
                    final Store idx = stores.getSecond();
                    final List<KeyValueTuple> nsList = new ArrayList<>();
                    final Cursor cursor = idx.openCursor(txn);
                    try {
                        ByteIterable valueEntry = cursor.getSearchKeyRange(LongBinding.longToCompressedEntry(timeStamp));
                        while (valueEntry != null) {
                            final String key = StringBinding.entryToString(valueEntry);
                            final long keyTimeStamp = LongBinding.compressedEntryToLong(cursor.getKey());
                            final ByteIterator itr = namespace.get(txn, valueEntry).iterator();
                            itr.skip(8); // ignore timestamp
                            final String value = IterableUtils.readString(itr);
                            valueEntry = cursor.getNext() ? cursor.getValue() : null;
                            nsList.add(new KeyValueTuple(key, value, keyTimeStamp));
                        }
                    } finally {
                        cursor.close();
                    }
                    result.add(new NameSpaceKVIterableTuple(ns, nsList));
                }
                return result;
            }
        });
    }


    private void replicateDoPost(String ns, String key, String value, Long timeStamp) {
        final App app = App.getInstance();
        for (int retry = 0; retry < app.replicationWriteRetryDegree; retry++) {
            final String[] friends = app.getFriends();
            app.shuffle(friends);
            if (friends.length == 0) {
                return;
            }
            final int maxFriends = Math.min(app.replicationWriteDegree, friends.length);
            final Map<Future, String> futureToFriends = new HashMap<>();
            AsyncQuorum.Context<ClientResponse, ClientResponse> ctx =
                    AsyncQuorum.createContext(Math.min(app.friendDegree, maxFriends),
                            new AsyncQuorum.ResultFilter<ClientResponse, ClientResponse>() {
                                @Nullable
                                @Override
                                public ClientResponse fold(@Nullable ClientResponse prev, @Nullable ClientResponse current) {
                                    return current;
                                }
                            }, new AsyncQuorum.ErrorHandler<ClientResponse>() {
                                @Override
                                public void handleFailed(Future<ClientResponse> failed, ExecutionException t) {
                                    final String friend = futureToFriends.get(failed);
                                    if (t == null) { // null means "cancelled"
                                        log.info("Replication cancelled for [" + friend + "]");
                                    } else {
                                        log.warn("Exception for [" + friend + "] " + t.getClass().getName() + ":" + t.getMessage());
                                        if (friend != null) {
                                            app.removeFriends(friend);
                                        }
                                    }
                                }
                            },
                            RemoteConnector.RESP_TYPE
                    );
            final Future[] futures = new Future[maxFriends];
            int i = 0;
            for (final String friend : friends) {
                log.info("Schedule replication to: " + friend);
                final Future<ClientResponse> future = RemoteConnector.getInstance().putAsync(friend, ns, key, value, ctx.getListener(), timeStamp);
                futures[i++] = future;
                futureToFriends.put(future, friend);
                if (i >= maxFriends) {
                    break;
                }
            }
            ctx.setFutures(futures);
            try {
                ctx.get(1000, TimeUnit.MILLISECONDS);
                ctx.cancel(true); // cancel other jobs
                log.warn("Replicated successfuly");
            } catch (QuorumException q) {
                log.warn("Replication quorum error, looks like a lot of friends went down: " + q.getMessage());
            } catch (TimeoutException t) {
                log.warn("Replication quorum timeout: " + t.getMessage());
            } catch (Throwable t) {
                log.error("Replication error", t);
            }
        }
    }

    public static Long putLocally(String ns, final String key, final String value, final Long timeStamp) {
        return App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<Long>() {
            @Override
            public Long compute(@NotNull final Transaction txn, @NotNull final Store namespace, @NotNull final Store idx, @NotNull final App app) {
                return putLocally(txn, namespace, idx, app, key, value, timeStamp);
            }
        });
    }

    public static Long putLocally(@NotNull final Transaction txn, @NotNull final Store namespace, @NotNull final Store idx,
                                  @NotNull final App app, @NotNull final String key, @NotNull final String value, @Nullable final Long timeStamp) {
        final ArrayByteIterable keyBytes = StringBinding.stringToEntry(key);
        final long nextTimeStamp = timeStamp == null ? System.currentTimeMillis() : timeStamp;
        final long oldTimeStamp;
        final ByteIterable oldValueBytes = namespace.get(txn, keyBytes);
        if (oldValueBytes == null) {
            oldTimeStamp = 0;
        } else {
            oldTimeStamp = IterableUtils.readLong(oldValueBytes.iterator());
            if (oldTimeStamp >= nextTimeStamp) {
                return null;
            }
        }
        final LightOutputStream out = new LightOutputStream(10 + value.length()); // 8 per long and 2 additional for str
        out.writeLong(nextTimeStamp);
        out.writeString(value);
        namespace.put(txn, keyBytes, out.asArrayByteIterable());
        if (oldTimeStamp != 0) {
            final Cursor cursor = idx.openCursor(txn);
            try {
                if (cursor.getSearchBoth(LongBinding.longToCompressedEntry(oldTimeStamp), keyBytes)) {
                    cursor.deleteCurrent();
                }
            } finally {
                cursor.close();
            }
        }
        idx.put(txn, LongBinding.longToCompressedEntry(nextTimeStamp), keyBytes);

        // update ns idx
        final Store namespacesIdx = app.getNamespacesIdx();
        final ArrayByteIterable nsKey = StringBinding.stringToEntry(namespace.getName());
        final ByteIterable oldTimeStampEntry = namespacesIdx.get(txn, nsKey);
        if (oldTimeStampEntry != null) {
            final long oldNsTimeStamp = LongBinding.compressedEntryToLong(oldTimeStampEntry);
            if (oldNsTimeStamp > nextTimeStamp) {
                return nextTimeStamp;
            }
        }
        namespacesIdx.put(txn, nsKey, LongBinding.longToCompressedEntry(nextTimeStamp));

        return nextTimeStamp;
    }
}
