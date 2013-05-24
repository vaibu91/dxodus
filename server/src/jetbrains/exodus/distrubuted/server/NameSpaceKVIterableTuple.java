package jetbrains.exodus.distrubuted.server;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public class NameSpaceKVIterableTuple {

    @NotNull
    private String namespace;
    @NotNull
    private List<KeyValueTuple> data;

    public NameSpaceKVIterableTuple() {
    }

    public NameSpaceKVIterableTuple(@NotNull final String namespace, @NotNull final List<KeyValueTuple> data) {
        this.namespace = namespace;
        this.data = data;
    }

    @NotNull
    public String getNamespace() {
        return namespace;
    }

    @NotNull
    public List<KeyValueTuple> getData() {
        return data;
    }
}
