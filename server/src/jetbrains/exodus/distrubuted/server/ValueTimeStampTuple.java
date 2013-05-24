package jetbrains.exodus.distrubuted.server;

import com.sun.javafx.beans.annotations.NonNull;

public class ValueTimeStampTuple {

    private long timeStamp;
    @NonNull
    private String value;

    public ValueTimeStampTuple() {
    }

    public ValueTimeStampTuple(long timeStamp, String value) {
        this.timeStamp = timeStamp;
        this.value = value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getValue() {
        return value;
    }
}
