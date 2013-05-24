package jetbrains.exodus.distrubuted.server;

import org.jetbrains.annotations.NotNull;

public class KeyValueTuple {

    @NotNull
    private final String key;
    @NotNull
    private final String value;
    private final long timeStamp;

    public KeyValueTuple(@NotNull final String key, @NotNull final String value, final long timeStamp) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    @NotNull
    public String getKey() {
        return key;
    }

    @NotNull
    public String getValue() {
        return value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
