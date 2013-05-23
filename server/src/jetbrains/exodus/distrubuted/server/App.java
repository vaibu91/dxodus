package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.net.httpserver.HttpServer;
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet;
import jetbrains.exodus.database.persistence.*;
import jetbrains.exodus.env.Environments;
import org.jetbrains.annotations.NotNull;

import javax.servlet.Filter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class App {

    private static App INSTANCE;
    private static final String[] EMPTY_FRIENDS = new String[0];

    private final URI baseURI;
    private final HttpServer server;
    private final Environment environment;
    private final Map<String, Store> namespaces = new HashMap<>();
    private final AtomicReference<PersistentHashSet<String>> friends = new AtomicReference<>();

    public App(URI baseURI, HttpServer server, Environment environment) {
        this.baseURI = baseURI;
        this.server = server;
        this.environment = environment;
    }

    public URI getBaseURI() {
        return baseURI;
    }

    public HttpServer getServer() {
        return server;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public <T> T computeInTransaction(@NotNull final String ns, @NotNull NamespaceTransactionalComputable<T> computable) {
        final Store[] store = new Store[1];
        final Transaction txn = environment.beginTransaction(new Runnable() {
            @Override
            public void run() {
                store[0] = namespaces.get(ns);
            }
        });
        txn.setCommitHook(new Runnable() {
            @Override
            public void run() {
                if (!namespaces.containsKey(ns)) { // don't remember if conflict occurred
                    namespaces.put(ns, store[0]);
                }
            }
        });
        try {
            while (true) {
                if (store[0] == null) {
                    store[0] = environment.openStore(ns, StoreConfiguration.WITHOUT_DUPLICATES, txn);
                }
                final T result = computable.compute(txn, store[0], this);
                if (txn.flush()) {
                    return result;
                }
                txn.revert();
            }
        } finally {
            txn.abort();
        }
    }

    public void close() {
        server.stop(0);
        for (Store store : namespaces.values()) {
            store.close();
        }
        environment.close();
        System.out.println("Server stopped");
    }

    public void addFriends(@NotNull final String... friends) {
        for (; ; ) {
            final PersistentHashSet<String> oldSet = this.friends.get();
            final PersistentHashSet<String> newSet = oldSet == null ? new PersistentHashSet<String>() : oldSet.getClone();
            final PersistentHashSet.MutablePersistentHashSet<String> mutableSet = newSet.beginWrite();
            for (final String friend : friends) {
                mutableSet.add(friend);
            }
            mutableSet.endWrite();
            if (this.friends.compareAndSet(oldSet, newSet)) {
                break;
            }
        }
    }

    public String[] getFriends() {
        final PersistentHashSet<String> friends = this.friends.get();
        if (friends == null) {
            return EMPTY_FRIENDS;
        }
        final PersistentHashSet.ImmutablePersistentHashSet<String> current = friends.getCurrent();
        final String[] result = new String[current.size()];
        int i = 0;
        for (final String friend : current) {
            result[i++] = friend;
        }
        return result;
    }

    public static App getInstance() {
        return INSTANCE;
    }

    public static void main(String[] args) {

        final EnvironmentConfig ec = new EnvironmentConfig();
        ec.setLogCacheShared(false);
        ec.setMemoryUsagePercentage(80);
        try {
            final Environment environment = Environments.newInstance(new File(System.getProperty("user.home"), "distrdata"), ec);
            final URI baseURI = URI.create(System.getProperty("dexodus.base.url", "http://localhost:8086/"));
            final HttpServer server = HttpServerFactory.create(baseURI, getResourceConfig());
            App.INSTANCE = new App(baseURI, server, environment);
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    getInstance().close();
                }
            }));
        } catch (IOException ex) {
            System.out.println("I/O Error");
            ex.printStackTrace();
        }

    }

    @SuppressWarnings("unchecked")
    public static ResourceConfig getResourceConfig() {
        final ClassNamesResourceConfig cfg = new ClassNamesResourceConfig(Database.class);
        cfg.getContainerResponseFilters().add(0, new CorsFilter());
        cfg.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        return cfg;
    }
}
