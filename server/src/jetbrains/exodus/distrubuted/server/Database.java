package jetbrains.exodus.distrubuted.server;

import jetbrains.exodus.database.ByteIterable;
import jetbrains.exodus.database.impl.bindings.StringBinding;
import jetbrains.exodus.database.impl.iterate.ArrayByteIterable;
import jetbrains.exodus.database.persistence.Store;
import jetbrains.exodus.database.persistence.Transaction;
import jetbrains.exodus.database.persistence.TransactionalComputable;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("")
@Produces(MediaType.TEXT_PLAIN)
@Consumes(MediaType.TEXT_PLAIN)
public class Database {

    @GET
    @Path("/{ns}/{key}")
    public String doGet(@PathParam("ns") final String ns, @PathParam("key") final String key) {
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
        return StringBinding.entryToString(valueBytes);
    }

    @POST
    @Path("/{ns}/{key}")
    public void doPost(@PathParam("ns") final String ns, @PathParam("key") final String key) {

    }
}
