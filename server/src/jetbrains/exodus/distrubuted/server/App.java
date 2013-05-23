package jetbrains.exodus.distrubuted.server;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.net.httpserver.HttpServer;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet;
import jetbrains.exodus.database.ByteIterable;
import jetbrains.exodus.database.impl.bindings.LongBinding;
import jetbrains.exodus.database.impl.bindings.StringBinding;
import jetbrains.exodus.database.persistence.*;
import jetbrains.exodus.env.Environments;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class App {

    private static App INSTANCE;
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final String NS_IDX_SUFFIX = "ns#idx";

    private final URI baseURI;
    private final HttpServer server;
    private final Environment environment;
    private final Random random = new SecureRandom();
    private final Map<String, Pair<Store, Store>> namespaces = new TreeMap<>();
    private final Store namespacesIdx;
    private final AtomicReference<PersistentHashSet<String>> friends = new AtomicReference<>();
    final int friendsToReplicatePut = Integer.getInteger("dexodus.friendsToReplicatePut", 2);

    public App(URI baseURI, HttpServer server, final Environment environment) {
        this.baseURI = baseURI;
        this.server = server;
        this.environment = environment;
        namespacesIdx = environment.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return environment.openStore(NS_IDX_SUFFIX, StoreConfiguration.WITHOUT_DUPLICATES, txn);
            }
        });
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

    public Random getRandom() {
        return random;
    }

    public Store getNamespacesIdx() {
        return namespacesIdx;
    }

    @SuppressWarnings("unchecked")
    public <T> T computeInTransaction(@NotNull final String ns, @NotNull NamespaceTransactionalComputable<T> computable) {
        final Pair<Store, Store>[] storePair = new Pair[1];
        final boolean[] localStores = new boolean[]{false};
        final Transaction txn = environment.beginTransaction(new Runnable() {
            @Override
            public void run() {
                storePair[0] = namespaces.get(ns);
            }
        });
        txn.setCommitHook(new Runnable() {
            @Override
            public void run() {
                if (!namespaces.containsKey(ns)) { // don't remember if conflict occurred
                    namespaces.put(ns, storePair[0]);
                    localStores[0] = false;
                }
            }
        });
        try {
            while (true) {
                if (storePair[0] == null) {
                    final Store store = environment.openStore(ns, StoreConfiguration.WITHOUT_DUPLICATES, txn);
                    final Store idx = environment.openStore(ns + NS_IDX_SUFFIX, StoreConfiguration.WITH_DUPLICATES, txn);
                    storePair[0] = new Pair<>(store, idx);
                    localStores[0] = true;
                }
                final T result = computable.compute(txn, storePair[0].getFirst(), storePair[0].getSecond(), this);
                if (txn.flush()) {
                    return result;
                }
                txn.revert();
            }
        } finally {
            txn.abort();
            if (localStores[0]) {
                storePair[0].getFirst().close();
                storePair[0].getSecond().close();
            }
        }
    }

    public Pair<Store, Store> getNsStores(@NotNull final String ns) {
        return computeInTransaction(ns, new NamespaceTransactionalComputable<Pair<Store, Store>>() {
            @Override
            public Pair<Store, Store> compute(@NotNull Transaction txn, @NotNull Store namespace, @NotNull Store idx, @NotNull App app) {
                return new Pair<>(namespace, idx);
            }
        });
    }

    @NotNull
    public String[] getNamespaces() {
        return environment.computeInTransaction(new TransactionalComputable<String[]>() {
            @Override
            public String[] compute(@NotNull final Transaction txn) {
                final List<String> nsList = environment.getAllStoreNames(txn);
                final int size = nsList.size();
                return size > 0 ? nsList.toArray(new String[size]) : EMPTY_STRING_ARRAY;
            }
        });
    }

    @NotNull
    public String[] getNamespaces(final long timestamp) {
        return environment.computeInTransaction(new TransactionalComputable<String[]>() {
            @Override
            public String[] compute(@NotNull final Transaction txn) {
                return getNamespaces(timestamp, txn);
            }
        });
    }

    @NotNull
    public String[] getNamespaces(final long timestamp, @NotNull final Transaction txn) {
        final List<String> result = new ArrayList<>();
        final List<String> nsList = environment.getAllStoreNames(txn);
        for (final String ns : nsList) {
            final ByteIterable timeStampEntry = namespacesIdx.get(txn, StringBinding.stringToEntry(ns));
            if (timeStampEntry == null) {
                throw new NullPointerException("There is no known timestamp for the namespace: " + ns);
            }
            if (LongBinding.compressedEntryToLong(timeStampEntry) >= timestamp) {
                result.add(ns);
            }
        }
        final int size = result.size();
        return size > 0 ? result.toArray(new String[size]) : EMPTY_STRING_ARRAY;
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

    public void removeFriends(@NotNull final String... friends) {
        for (; ; ) {
            final PersistentHashSet<String> oldSet = this.friends.get();
            final PersistentHashSet<String> newSet = oldSet == null ? new PersistentHashSet<String>() : oldSet.getClone();
            final PersistentHashSet.MutablePersistentHashSet<String> mutableSet = newSet.beginWrite();
            for (final String friend : friends) {
                mutableSet.remove(friend);
            }
            mutableSet.endWrite();
            if (this.friends.compareAndSet(oldSet, newSet)) {
                break;
            }
        }
    }

    @NotNull
    public String[] getFriends() {
        final PersistentHashSet<String> friends = this.friends.get();
        if (friends == null) {
            return EMPTY_STRING_ARRAY;
        }
        final PersistentHashSet.ImmutablePersistentHashSet<String> current = friends.getCurrent();
        final String[] result = new String[current.size()];
        int i = 0;
        for (final String friend : current) {
            result[i++] = friend;
        }
        return result;
    }

    public void close() {
        server.stop(0);
        for (final Pair<Store, Store> storePair : namespaces.values()) {
            storePair.getFirst().close();
            storePair.getSecond().close();
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
            final URI baseURI = URI.create(System.getProperty("dexodus.base.url", "http://localhost:8086/"));
            final HttpServer server = HttpServerFactory.create(baseURI, getResourceConfig());
            App.INSTANCE = new App(baseURI, server, environment);
            App.getInstance().addFriends(parseFriends());
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

    private static String[] parseFriends() {
        final String friends = System.getProperty("dexodus.friends");
        if (friends == null) {
            return EMPTY_STRING_ARRAY;
        }
        return friends.split(",");
    }
}
