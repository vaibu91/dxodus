package jetbrains.exodus.distrubuted.server;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("")
@Produces(MediaType.TEXT_PLAIN)
@Consumes(MediaType.TEXT_PLAIN)
public class Database {

    @GET
    @Path("/{ns}/{key}")
    public String doGet(@PathParam("ns") final String ns, @PathParam("key") final String key) {
        return ns + "/" + key;
    }

    @POST
    @Path("/{ns}/{key}")
    public void doPost(@PathParam("ns") final String ns, @PathParam("key") final String key) {

    }
}
