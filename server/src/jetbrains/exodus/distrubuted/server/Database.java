package jetbrains.exodus.distrubuted.server;

import jetbrains.exodus.database.ByteIterable;
import jetbrains.exodus.database.ByteIterator;
import jetbrains.exodus.database.impl.bindings.StringBinding;
import jetbrains.exodus.database.impl.iterate.ArrayByteIterable;
import jetbrains.exodus.database.impl.iterate.IterableUtils;
import jetbrains.exodus.database.impl.iterate.LightOutputStream;
import jetbrains.exodus.database.persistence.Store;
import jetbrains.exodus.database.persistence.Transaction;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("")
@Produces(MediaType.TEXT_PLAIN)
@Consumes(MediaType.TEXT_PLAIN)
public class Database {

    @GET
    @Path("/{ns}/{key}")
    public String doGet(@PathParam("ns") final String ns, @PathParam("key") final String key,
                        @QueryParam("timeStamp") final Long timeStamp) {
        System.out.println("TS: " + timeStamp);
        final ArrayByteIterable keyBytes = StringBinding.stringToEntry(key);
        final ByteIterable valueBytes = App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<ByteIterable>() {
            @Override
            public ByteIterable compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull App app) {
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
    public Response doPost(@PathParam("ns") final String ns, @PathParam("key") final String key,
                           @FormParam("value") final String value, @QueryParam("timeStamp") final Long timeStamp,
                           @Context UriInfo uriInfo) {
        System.out.println("POST");
        final ArrayByteIterable keyBytes = StringBinding.stringToEntry(key);
        final Long nextTimeStamp = App.getInstance().computeInTransaction(ns, new NamespaceTransactionalComputable<Long>() {
            @Override
            public Long compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull App app) {
                final long nextTimeStamp = timeStamp == null ? System.currentTimeMillis() : timeStamp;
                final ByteIterable oldValueBytes = namespace.get(txn, keyBytes);
                if (oldValueBytes != null) {
                    final long oldTimeStamp = IterableUtils.readLong(oldValueBytes.iterator());
                    if (oldTimeStamp > nextTimeStamp) {
                        return null;
                    }
                }
                LightOutputStream out = new LightOutputStream(10 + value.length()); // 8 per long and 2 additional for str
                out.writeLong(nextTimeStamp);
                out.writeString(value);
                namespace.put(txn, keyBytes, out.asArrayByteIterable());
                return nextTimeStamp;
            }
        });
        if (nextTimeStamp == null) {
            return Response.status(Response.Status.NOT_ACCEPTABLE).build();
        }
        return Response.created(uriInfo.getBaseUriBuilder().path(Database.class).
                fragment("{ns}").fragment("{key}").fragment("{timeStamp}").build(ns, key, nextTimeStamp)).build();
    }
}
