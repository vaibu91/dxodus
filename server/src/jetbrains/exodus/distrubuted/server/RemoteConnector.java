package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.client.*;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RemoteConnector {

    private static final RemoteConnector INSTANCE = new RemoteConnector();
    private static final GenericType<String> STRING_TYPE = new GenericType<>(String.class);
    private static final GenericType<String[]> STRING_ARR_TYPE = new GenericType<>(String[].class);
    private static final GenericType<ClientResponse> RESP_TYPE = new GenericType<>(ClientResponse.class);

    private final Client c;

    public RemoteConnector() {
        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        clientConfig.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, Boolean.TRUE);
        c = Client.create(clientConfig);
    }

    public String get(@NotNull final String url, @NotNull final String ns, @NotNull final String key, long timeout) throws TimeoutException {
        return get(url, ns, key, timeout, null);
    }

    public String get(@NotNull final String url, @NotNull final String ns,
                      @NotNull final String key, long timeout, @Nullable final Long timeStamp) throws TimeoutException {
        return wrapFuture(timeout, getAsync(url, ns, key, timeStamp));
    }

    public Future<String> getAsync(@NotNull final String url, @NotNull final String ns, @NotNull final String key) {
        return getAsync(url, ns, key, null);
    }

    public Future<String> getAsync(@NotNull final String url, @NotNull final String ns,
                                   @NotNull final String key, @Nullable final Long timeStamp) {
        AsyncWebResource r = c.asyncResource(url + ns + '/' + key);
        if (timeStamp != null) {
            r = r.queryParam("timeStamp", timeStamp.toString());
        }
        return r.get(STRING_TYPE);
    }

    public ClientResponse put(@NotNull final String url, @NotNull final String ns,
                              @NotNull final String key, @NotNull String value, long timeout) throws TimeoutException {
        return put(url, ns, key, value, timeout, null);
    }

    public ClientResponse put(@NotNull final String url, @NotNull final String ns, @NotNull final String key,
                              @NotNull String value, long timeout, @Nullable final Long timeStamp) throws TimeoutException {
        return wrapFuture(timeout, putAsync(url, ns, key, value, timeStamp));
    }

    @NotNull
    public Future<ClientResponse> putAsync(@NotNull final String url, @NotNull final String ns, @NotNull final String key,
                                           @NotNull String value) {
        return putAsync(url, ns, key, value, null);
    }

    @NotNull
    public Future<ClientResponse> putAsync(@NotNull final String url, @NotNull final String ns, @NotNull final String key,
                                           @NotNull String value, @Nullable final Long timeStamp) {
        AsyncWebResource r = c.asyncResource(url + ns + '/' + key);
        if (timeStamp != null) {
            r = r.queryParam("timeStamp", timeStamp.toString());
        }
        r.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
        final MultivaluedMap<String, String> formData = new MultivaluedMapImpl();
        formData.add("value", value);
        return r.post(RESP_TYPE, formData);
    }

    public String[] friends(@NotNull final String url, long timeout) throws TimeoutException {
        return wrapFuture(timeout, friendsAsync(url));
    }

    private Future<String[]> friendsAsync(String url) {
        AsyncWebResource r = c.asyncResource(url + "friends");
        return r.get(STRING_ARR_TYPE);
    }

    public static RemoteConnector getInstance() {
        return INSTANCE;
    }

    public static void main(String[] args) throws TimeoutException {
        // simple connector test
        final String val = Long.toBinaryString(System.currentTimeMillis());
        final String url = "http://localhost:8086/";
        final RemoteConnector conn = RemoteConnector.getInstance();
        System.out.println(conn.put(url, "ns1", "key2", val, 1000).getStatus());
        System.out.println(conn.get(url, "ns1", "key2", 1000));
        System.out.println(Arrays.toString(conn.friends(url, 1000)));
    }

    private static <T> T wrapFuture(final long timeout, @NotNull final Future<T> future) throws TimeoutException {
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException t) {
            System.out.println(future.cancel(true) ? "Cancelled future" : "Timed out future");
            throw t;
        }
    }
}
