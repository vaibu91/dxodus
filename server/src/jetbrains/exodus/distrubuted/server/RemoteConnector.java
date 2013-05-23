package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

public class RemoteConnector {

    private static final RemoteConnector INSTANCE = new RemoteConnector();
    private static final GenericType<String> STRING_TYPE = new GenericType<>(String.class);
    private static final GenericType<ClientResponse> RESP_TYPE = new GenericType<>(ClientResponse.class);

    public String get(@NotNull final String url, @NotNull final String ns, @NotNull final String key) {
        return get(url, ns, key, null);
    }

    public String get(@NotNull final String url, @NotNull final String ns,
                      @NotNull final String key, @Nullable final Long timeStamp) {
        Client c = createClient();
        WebResource r = c.resource(url + ns + '/' + key);
        if (timeStamp != null) {
            r = r.queryParam("timeStamp", timeStamp.toString());
        }
        return r.get(STRING_TYPE);
    }

    public ClientResponse put(@NotNull final String url, @NotNull final String ns,
                        @NotNull final String key, @NotNull String value) {
        return put(url, ns, key, value, null);
    }

    public ClientResponse put(@NotNull final String url, @NotNull final String ns,
                      @NotNull final String key, @NotNull String value, @Nullable final Long timeStamp) {
        Client c = createClient();
        WebResource r = c.resource(url + ns + '/' + key);
        if (timeStamp != null) {
            r = r.queryParam("timeStamp", timeStamp.toString());
        }
        r.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
        final MultivaluedMap<String, String> formData = new MultivaluedMapImpl();
        formData.add("value", value);
        return r.post(RESP_TYPE, formData);
    }

    private Client createClient() {
        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        clientConfig.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, Boolean.TRUE);
        return Client.create(clientConfig);
    }

    public static RemoteConnector getInstance() {
        return INSTANCE;
    }

    public static void main(String[] args) {
        // simple connector test
        final String val = Long.toBinaryString(System.currentTimeMillis());
        System.out.println(RemoteConnector.getInstance().put("http://localhost:8086/", "ns1", "key2", val).getStatus());
        System.out.println(RemoteConnector.getInstance().get("http://localhost:8086/", "ns1", "key2"));
    }
}
