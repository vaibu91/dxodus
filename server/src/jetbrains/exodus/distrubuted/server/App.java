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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class App {

    private static Logger log = LoggerFactory.getLogger(App.class);

    private static App INSTANCE;
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final String NS_IDX_SUFFIX = "ns#idx";

    private static int[] ports = new int[]{8080, 8082, 8888, 8089, 8087, 8086};

    private final URI baseURI;
    private final HttpServer server;
    private final Environment environment;
    private final Random random = new SecureRandom();
    private final Map<String, Pair<Store, Store>> namespaces = new TreeMap<>();
    private final Store namespacesIdx;
    private final AtomicReference<PersistentHashSet<String>> friends = new AtomicReference<>();
    private final AtomicReference<PersistentHashSet<FriendsListener>> friendListeners = new AtomicReference<>();

    final int friendDegree = Integer.getInteger("dexodus.friendDegree", 2);
    final int replicationReadDegree = Integer.getInteger("dexodus.replicationReadDegree", 4);
    final int replicationWriteDegree = Integer.getInteger("dexodus.replicationWriteDegree", 4);
    final int replicationWriteRetryDegree = Integer.getInteger("dexodus.replicationWriteRetryDegree", 3);

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
        final Pair<Store, Store>[] storePair = new Pair[]{null};
        final boolean[] localStores = new boolean[]{false};
        final Transaction txn = environment.beginTransaction(new Runnable() {
            @Override
            public void run() {
                if (storePair[0] == null) {
                    storePair[0] = namespaces.get(ns);
                }
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
                return getNamespaces(txn);
            }
        });
    }

    @NotNull
    public String[] getNamespaces(@NotNull final Transaction txn) {
        final List<String> result = new ArrayList<>();
        final List<String> nsList = environment.getAllStoreNames(txn);
        for (final String ns : nsList) {
            if (!ns.endsWith(NS_IDX_SUFFIX)) {
                result.add(ns);
            }
        }
        final int size = result.size();
        return size > 0 ? result.toArray(new String[size]) : EMPTY_STRING_ARRAY;
    }

    @NotNull
    public String[] getNamespaces(final long timestamp, @NotNull final Transaction txn) {
        final List<String> result = new ArrayList<>();
        final List<String> nsList = environment.getAllStoreNames(txn);
        for (final String ns : nsList) {
            if (!ns.endsWith(NS_IDX_SUFFIX)) {
                final ByteIterable timeStampEntry = namespacesIdx.get(txn, StringBinding.stringToEntry(ns));
                // throw new NullPointerException("There is no known timestamp for the namespace: " + ns);
                if (timeStampEntry != null && LongBinding.compressedEntryToLong(timeStampEntry) >= timestamp) {
                    result.add(ns);
                }
            }
        }
        final int size = result.size();
        return size > 0 ? result.toArray(new String[size]) : EMPTY_STRING_ARRAY;
    }

    public void addFriends(@NotNull final String... friends) {
        final List<String> added = new ArrayList<>();
        for (; ; ) {
            added.clear();
            final PersistentHashSet<String> oldSet = this.friends.get();
            final PersistentHashSet<String> newSet = oldSet == null ? new PersistentHashSet<String>() : oldSet.getClone();
            final PersistentHashSet.MutablePersistentHashSet<String> mutableSet = newSet.beginWrite();
            for (final String friend : friends) {
                // do not make friends with yourself
                if (!URI.create(friend).equals(getBaseURI())) {
                    if (!mutableSet.contains(friend)) {
                        mutableSet.add(friend);
                        added.add(friend);
                    }
                }
            }
            mutableSet.endWrite();
            if (this.friends.compareAndSet(oldSet, newSet)) {
                break;
            }
        }
        final PersistentHashSet<FriendsListener> listeners = friendListeners.get();
        for (final String friend : added) {
            log.info("Add friend [" + friend + "]");
            if (listeners != null) {
                for (final FriendsListener listener : listeners.getCurrent()) {
                    listener.friendAdded(friend);
                }
            }
        }
    }

    public void removeFriends(@NotNull final String... friends) {
        final List<String> removed = new ArrayList<>();
        for (; ; ) {
            removed.clear();
            final PersistentHashSet<String> oldSet = this.friends.get();
            if (oldSet == null) {
                break;
            }
            final PersistentHashSet<String> newSet = oldSet.getClone();
            final PersistentHashSet.MutablePersistentHashSet<String> mutableSet = newSet.beginWrite();
            for (final String friend : friends) {
                if (mutableSet.remove(friend)) {
                    removed.add(friend);
                }
            }
            mutableSet.endWrite();
            if (this.friends.compareAndSet(oldSet, newSet)) {
                break;
            }
        }
        final PersistentHashSet<FriendsListener> listeners = friendListeners.get();
        for (final String friend : removed) {
            log.info("Remove friend [" + friend + "]");
            if (listeners != null) {
                for (final FriendsListener listener : listeners.getCurrent()) {
                    listener.friendRemoved(friend);
                }
            }
        }
    }

    public void addFriendsListener(@NotNull final FriendsListener listener) {
        for (; ; ) {
            final PersistentHashSet<FriendsListener> oldSet = friendListeners.get();
            final PersistentHashSet<FriendsListener> newSet = oldSet == null ? new PersistentHashSet<FriendsListener>() : oldSet.getClone();
            final PersistentHashSet.MutablePersistentHashSet<FriendsListener> mutableSet = newSet.beginWrite();
            mutableSet.add(listener);
            mutableSet.endWrite();
            if (friendListeners.compareAndSet(oldSet, newSet)) {
                break;
            }
        }
    }

    public void removeFriendsListener(@NotNull final FriendsListener listener) {
        for (; ; ) {
            final PersistentHashSet<FriendsListener> oldSet = friendListeners.get();
            if (oldSet == null) {
                break;
            }
            final PersistentHashSet<FriendsListener> newSet = oldSet.getClone();
            final PersistentHashSet.MutablePersistentHashSet<FriendsListener> mutableSet = newSet.beginWrite();
            mutableSet.remove(listener);
            mutableSet.endWrite();
            if (friendListeners.compareAndSet(oldSet, newSet)) {
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

    public void shuffle(String[] data) {
        if (data.length < 2) {
            return;
        }
        for (int i = data.length - 1; i >= 0; i--) {
            final int index = random.nextInt(i + 1);
            final String a = data[index];
            data[index] = data[i];
            data[i] = a;
        }
    }

    public void close() {
        server.stop(0);
        for (final Pair<Store, Store> storePair : namespaces.values()) {
            storePair.getFirst().close();
            storePair.getSecond().close();
        }
        namespacesIdx.close();
        environment.close();
        log.info("Server stopped");
    }

    public static App getInstance() {
        return INSTANCE;
    }

    public static void main(String[] args) {
        final EnvironmentConfig ec = new EnvironmentConfig();
        ec.setLogCacheShared(false);
        ec.setMemoryUsagePercentage(80);
        try {
            // database
            Environment environment = null;
            String dir = null;
            for (int i = 0; i < 10; i++) {
                try {
                    dir = "distrdata" + (i == 0 ? "" : String.valueOf(i));
                    environment = Environments.newInstance(new File(System.getProperty("user.home"), dir), ec);
                    break;
                } catch (Exception e) {
                    log.info("Can not start db for dir [" + dir + "] " + e.getMessage());
                }
            }
            if (environment == null) {
                return;
            }

            // http server
            String baseUrl = System.getProperty("dexodus.base.url");
            URI baseURI = null;
            HttpServer server = null;

            if (baseUrl == null) {
                // get own ip
                for (int p : ports) {
                    try {
                        baseUrl = "http://" + Utils.getLocalAddress().getHostAddress() + ":" + p + "/";
                        baseURI = URI.create(baseUrl);
                        server = HttpServerFactory.create(baseURI, getResourceConfig());
                        break;
                    } catch (Exception e) {
                        log.info("Can not start server on port " + p + ": " + e.getMessage());
                    }
                }

                if (server == null) {
                    return;
                }
            } else {
                baseURI = URI.create(baseUrl);
                server = HttpServerFactory.create(baseURI, getResourceConfig());
            }

            App.INSTANCE = new App(baseURI, server, environment);
            final BackgroundReplicator backgroundReplicator = new BackgroundReplicator();
            App.getInstance().addFriends(parseFriends());
            server.start();
            log.info("Start server " + baseURI.toString());

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    backgroundReplicator.close();
                    getInstance().close();
                }
            }));

            FriendsDiscovery.getInstance().discoverFriends();

            loadSourcesIfNecessary();

        } catch (IOException ex) {
            log.info("I/O Error");
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

    private static void loadSourcesIfNecessary() {
        final String sourceDir = System.getProperty("dexodus.sourcePath");
        if (sourceDir != null) {
            final File sourcePath = new File(sourceDir);
            if (sourcePath.isDirectory()) {
                loadPathSources(sourcePath);
            }
        }
    }

    private static void loadPathSources(File sourcePath) {
        final char[] buf = new char[2048];
        for (final File javaFile : sourcePath.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".java");
            }
        })) {
            log.info("Loading [" + javaFile + "]");
            try {
                final StringBuilder contentBuilder = new StringBuilder((int) javaFile.length());
                try (final FileReader reader = new FileReader(javaFile)) {
                    int read;
                    while ((read = reader.read(buf)) != -1) {
                        contentBuilder.append(buf, 0, read);
                    }
                }
                Database.putLocally("java", javaFile.getName(), contentBuilder.toString(), System.currentTimeMillis());
            } catch (IOException ioe) {
                log.warn("Failed to load " + javaFile);
            }
        }
        for (final File path : sourcePath.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        })) {
            loadPathSources(path);
        }
    }

    public static interface FriendsListener {

        void friendAdded(@NotNull final String friend);

        void friendRemoved(@NotNull final String friend);
    }
}
