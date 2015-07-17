package voldemort.store.readonly.swapper;

import org.apache.log4j.Logger;

/**
 * A logger implementation which prepends a string to all of its logs.
 */
public class PrefixedLogger extends Logger {
    private final String prefix;
    protected PrefixedLogger(String name, String prefix) {
        super(name);
        this.prefix = prefix;
    }
    public static Logger getLogger(String name, String prefix) {
        return Logger.getLogger(name, new PrefixedLoggerFactory(prefix));
    }
    @Override
    public void error(Object message, Throwable throwable) {
        super.error(prefix + " " + message, throwable);
    }
    @Override
    public void error(Object message) {
        super.error(prefix + " " + message);
    }
    @Override
    public void info(Object message, Throwable throwable) {
        super.info(prefix + " " + message, throwable);
    }
    @Override
    public void info(Object message) {
        super.info(prefix + " " + message);
    }
    @Override
    public void warn(Object message, Throwable throwable) {
        super.warn(prefix + " " + message, throwable);
    }
    @Override
    public void warn(Object message) {
        super.warn(prefix + " " + message);
    }
    @Override
    public void debug(Object message, Throwable throwable) {
        super.debug(prefix + " " + message, throwable);
    }
    @Override
    public void debug(Object message) {
        super.debug(prefix + " " + message);
    }
    @Override
    public void trace(Object message, Throwable throwable) {
        super.trace(prefix + " " + message, throwable);
    }
    @Override
    public void trace(Object message) {
        super.trace(prefix + " " + message);
    }
    @Override
    public void fatal(Object message, Throwable throwable) {
        super.fatal(prefix + " " + message, throwable);
    }
    @Override
    public void fatal(Object message) {
        super.fatal(prefix + " " + message);
    }
}
