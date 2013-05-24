package jetbrains.exodus.distrubuted.server;

import org.jetbrains.annotations.NotNull;

public class NameSpaceKVIterableTuple {

    @NotNull
    private final String namespace;
    @NotNull
    private final Iterable<KeyValueTuple> data;

    public NameSpaceKVIterableTuple(@NotNull final String namespace, @NotNull final Iterable<KeyValueTuple> data) {
        this.namespace = namespace;
        this.data = data;
    }

    @NotNull
    public String getNamespace() {
        return namespace;
    }

    @NotNull
    public Iterable<KeyValueTuple> getData() {
        return data;
    }
}
