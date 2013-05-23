package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.net.httpserver.HttpServer;
import jetbrains.exodus.database.persistence.*;
import jetbrains.exodus.env.Environments;
import org.jetbrains.annotations.NotNull;

import javax.servlet.Filter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class App {

    private static App INSTANCE;

    private final HttpServer server;
    private final Environment environment;
    private final Map<String, Store> namespaces = new HashMap<>();

    public App(HttpServer server, Environment environment) {
        this.server = server;
        this.environment = environment;
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

    public static App getInstance() {
        return INSTANCE;
    }

    public static void main(String[] args) {
        final EnvironmentConfig ec = new EnvironmentConfig();
        ec.setLogCacheShared(false);
        ec.setMemoryUsagePercentage(80);
        try {
            final Environment environment = Environments.newInstance(new File(System.getProperty("user.home"), "distrdata"), ec);
            final HttpServer server = HttpServerFactory.create(
                    URI.create(System.getProperty("dexodus.base.url", "http://localhost:8086/")), getResourceConfig());
            App.INSTANCE = new App(server, environment);
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
        return cfg;
    }
}
