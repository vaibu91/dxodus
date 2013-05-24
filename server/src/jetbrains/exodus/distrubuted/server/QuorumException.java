package jetbrains.exodus.distrubuted.server;

public class QuorumException extends RuntimeException {

    public QuorumException(String message) {
        super(message);
    }
}
