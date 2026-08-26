package linkmesh.proto;

import linkmesh.common.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Accepts connections and serves messages until the peer disconnects.
 *
 * One virtual thread per connection. Connections are long-lived and mostly idle,
 * so 50 nodes cost 50 parked continuations instead of 50 OS threads. Page
 * parsing stays on platform threads since it is CPU-bound.
 */
public final class MessageServer implements AutoCloseable {
    private static final Log log = Log.of("server");
    private static final int IDLE_POLL_MS = 120_000;

    /** Return null from handle() to send no reply (fire-and-forget verbs). */
    public interface Handler {
        Message handle(Message request, Connection connection) throws Exception;

        default void onConnect(Connection connection) {}

        default void onDisconnect(Connection connection) {}
    }

    private final int requestedPort;
    private final Handler handler;
    private final String name;
    private final ExecutorService connectionThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Connection> live = ConcurrentHashMap.newKeySet();

    private volatile ServerSocket serverSocket;
    private volatile Thread acceptThread;
    private volatile boolean closed;

    public MessageServer(String name, int port, Handler handler) {
        this.name = name;
        this.requestedPort = port;
        this.handler = handler;
    }

    /** Binds and starts accepting. Port 0 asks the OS for a free port. */
    public int start() throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress("0.0.0.0", requestedPort));
        this.serverSocket = socket;
        acceptThread = Thread.ofPlatform().name(name + "-accept").start(this::acceptLoop);
        return socket.getLocalPort();
    }

    public int port() {
        ServerSocket socket = serverSocket;
        return socket == null ? -1 : socket.getLocalPort();
    }

    public int liveConnections() { return live.size(); }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket socket = serverSocket.accept();
                connectionThreads.submit(() -> serve(socket));
            } catch (IOException e) {
                if (!closed) log.warn("accept failed: %s", e.getMessage());
            }
        }
    }

    private void serve(Socket socket) {
        Connection connection = null;
        try {
            connection = Connection.accept(socket, IDLE_POLL_MS);
            live.add(connection);
            handler.onConnect(connection);
            pump(connection);
        } catch (IOException e) {
            log.debug("connection ended: %s", e.getMessage());
        } finally {
            if (connection != null) {
                live.remove(connection);
                try {
                    handler.onDisconnect(connection);
                } catch (RuntimeException e) {
                    log.warn("disconnect handler failed: %s", e.getMessage());
                }
                connection.close();
            }
        }
    }

    private void pump(Connection connection) throws IOException {
        while (!closed) {
            Message request;
            try {
                request = connection.receive();
            } catch (SocketTimeoutException e) {
                // Idle connection, not a failure. Keep waiting.
                continue;
            }
            if (request == null) return;

            Message reply;
            try {
                reply = handler.handle(request, connection);
            } catch (IllegalArgumentException | ProtocolException e) {
                reply = Message.error(e.getMessage() == null ? e.toString() : e.getMessage());
            } catch (Exception e) {
                log.warn("handler threw on %s: %s: %s", request.verb(), e.getClass().getSimpleName(), e.getMessage());
                reply = Message.error(e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            if (reply != null) connection.send(reply);
        }
    }

    @Override
    public void close() {
        closed = true;
        ServerSocket socket = serverSocket;
        if (socket != null) {
            try { socket.close(); } catch (IOException ignored) { }
        }
        for (Connection connection : live) connection.close();
        live.clear();
        connectionThreads.shutdownNow();
        Thread thread = acceptThread;
        if (thread != null) thread.interrupt();
    }

    /** Ignore SocketException noise emitted while shutting down. */
    static boolean isShutdownNoise(IOException e) {
        return e instanceof SocketException && String.valueOf(e.getMessage()).contains("closed");
    }
}
