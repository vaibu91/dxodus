package jetbrains.exodus.distrubuted.server;

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
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("")
@Produces(MediaType.APPLICATION_JSON)
public class Database {

    @GET
    @Path("/{ns}/{key}")
    public String doGet(@PathParam("ns") final String ns, @PathParam("key") final String key,
                        @QueryParam("timeStamp") final Long timeStamp) {
        System.out.println("TS: " + timeStamp);
        final ArrayByteIterable keyBytes = StringBinding.stringToEntry(key);
        final ByteIterable valueBytes = App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<ByteIterable>() {
            @Override
            public ByteIterable compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull Store idx, @NotNull App app) {
                return namespace.get(txn, keyBytes);
            }
        });
        if (valueBytes == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        final ByteIterator itr = valueBytes.iterator();
        itr.skip(8); // ignore timestamp
        return IterableUtils.readString(itr);
    }

    @POST
    @Path("/{ns}/{key}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response doPost(@PathParam("ns") final String ns, @PathParam("key") final String key,
                           @FormParam("value") final String value, @QueryParam("timeStamp") final Long timeStamp,
                           @Context UriInfo uriInfo) {
        System.out.println("POST to " + App.getInstance().getBaseURI().toString());
        final ArrayByteIterable keyBytes = StringBinding.stringToEntry(key);
        final Long nextTimeStamp = App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<Long>() {
            @Override
            public Long compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull Store idx, @NotNull App app) {
                final long nextTimeStamp = timeStamp == null ? System.currentTimeMillis() : timeStamp;
                final long oldTimeStamp;
                final ByteIterable oldValueBytes = namespace.get(txn, keyBytes);
                if (oldValueBytes == null) {
                    oldTimeStamp = 0;
                } else {
                    oldTimeStamp = IterableUtils.readLong(oldValueBytes.iterator());
                    if (oldTimeStamp > nextTimeStamp) {
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
                return nextTimeStamp;
            }
        });
        if (nextTimeStamp == null) {
            return Response.status(Response.Status.NOT_ACCEPTABLE).build();
        }
        return Response.created(uriInfo.getBaseUriBuilder().path(Database.class).
                fragment("{ns}").fragment("{key}").fragment("{timeStamp}").build(ns, key, nextTimeStamp)).build();
    }

    @GET
    @Path("/namespaces")
    public String[] doGetNamespaces() {
        return App.getInstance().getNamespaces();
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
}
