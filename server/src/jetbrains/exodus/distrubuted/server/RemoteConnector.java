package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class RemoteConnector {

    private static RemoteConnector INSTANCE = new RemoteConnector();

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
        return r.get(new GenericType<String>(String.class));
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
        System.out.println(RemoteConnector.getInstance().get("http://localhost:8086/", "ns1", "key2"));
    }
}
