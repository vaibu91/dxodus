package jetbrains.exodus.distrubuted.server;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public class NameSpaceKVListTuple {

    @NotNull
    private final String namespace;
    @NotNull
    private final List<KeyValueTuple> data;

    public NameSpaceKVListTuple(@NotNull final String namespace, @NotNull final List<KeyValueTuple> data) {
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
