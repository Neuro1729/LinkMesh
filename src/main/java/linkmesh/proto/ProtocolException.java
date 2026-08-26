package linkmesh.proto;

import java.io.IOException;

/** Thrown when a peer replies ERR or sends a frame we cannot parse. */
public class ProtocolException extends RuntimeException {
    public ProtocolException(String message) { super(message); }

    public ProtocolException(String message, Throwable cause) { super(message, cause); }

    public static ProtocolException wrap(IOException e) {
        return new ProtocolException(e.getMessage() == null ? e.toString() : e.getMessage(), e);
    }
}
