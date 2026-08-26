package linkmesh.common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Tiny leveled logger. No dependencies, one line per event, greppable prefixes. */
public final class Log {
    public enum Level { DEBUG, INFO, WARN, ERROR }

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static volatile Level threshold = Level.INFO;

    private final String component;

    private Log(String component) { this.component = component; }

    public static Log of(String component) { return new Log(component); }

    public static void setLevel(Level level) { threshold = level; }

    public static Level level() { return threshold; }

    public void debug(String format, Object... args) { emit(Level.DEBUG, format, args); }
    public void info(String format, Object... args) { emit(Level.INFO, format, args); }
    public void warn(String format, Object... args) { emit(Level.WARN, format, args); }
    public void error(String format, Object... args) { emit(Level.ERROR, format, args); }

    private void emit(Level level, String format, Object... args) {
        if (level.ordinal() < threshold.ordinal()) return;
        String message = args.length == 0 ? format : String.format(format, args);
        String line = String.format("%s %-5s [%s] %s",
                LocalTime.now().format(TIME), level, component, message);
        if (level.ordinal() >= Level.WARN.ordinal()) System.err.println(line);
        else System.out.println(line);
    }

    /** Metrics print on a stable prefix so scripts can grep them out of logs. */
    public static void metric(String name, Object value) {
        System.out.printf("METRIC %s=%s%n", name, value);
    }
}
