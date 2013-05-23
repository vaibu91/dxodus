package jetbrains.exodus.distrubuted.server;

public class KeyValueTuple {

    private final String key;
    private final String value;
    private final long timeStamp;

    public KeyValueTuple(String key, String value, long timeStamp) {
        this.key = key;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
