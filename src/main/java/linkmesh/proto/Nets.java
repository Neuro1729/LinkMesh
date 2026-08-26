package linkmesh.proto;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Picks the address other machines should use to reach this one.
 *
 * The old design made every worker pass --host explicitly, and passing the
 * default 127.0.0.1 on a real LAN silently produced a cluster where the
 * controller dialled itself. Detecting the address removes that whole class of
 * misconfiguration; --advertise still overrides when detection guesses wrong
 * (multi-homed hosts, containers, VPNs).
 */
public final class Nets {
    private Nets() {}

    public static String detectAdvertiseAddress() {
        List<InetAddress> candidates = new ArrayList<>();
        try {
            for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!nic.isUp() || nic.isLoopback() || nic.isVirtual()) continue;
                String name = nic.getDisplayName() == null ? "" : nic.getDisplayName().toLowerCase();
                if (name.contains("virtualbox") || name.contains("vmware") || name.contains("hyper-v")
                        || name.contains("docker") || name.contains("loopback")) continue;
                for (InetAddress address : Collections.list(nic.getInetAddresses())) {
                    if (address instanceof Inet4Address && !address.isLoopbackAddress()) {
                        candidates.add(address);
                    }
                }
            }
        } catch (SocketException ignored) {
            // Fall through to the loopback default below.
        }
        for (InetAddress address : candidates) {
            if (address.isSiteLocalAddress()) return address.getHostAddress();
        }
        if (!candidates.isEmpty()) return candidates.get(0).getHostAddress();
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    /** Stable default node id: hostname plus port, so restarts reclaim their identity. */
    public static String defaultNodeId(int port) {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "node";
        }
        host = host.replaceAll("[^A-Za-z0-9_-]", "-").toLowerCase();
        return host + "-" + port;
    }
}
