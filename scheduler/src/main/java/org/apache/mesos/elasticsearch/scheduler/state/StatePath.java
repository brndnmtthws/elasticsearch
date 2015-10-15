package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;

/**
 * Path utilities
 */
public class StatePath {
    private static final Logger LOGGER = Logger.getLogger(StatePath.class);
    private SerializableState zkState;
    public StatePath(SerializableState zkState) {
        this.zkState = zkState;
    }

    /**
     * Creates the zNode if it does not exist. Will create parent directories.
     * @param key the zNode path
     */
    // Where `key` has N components, this makes:
    //   N GET requests
    //   [0,N] SET requests
    public void mkdir(String key) throws IOException {
        key = key.replace(" ", "");
        if (key.endsWith("/") && !key.equals("/")) {
            throw new InvalidParameterException("Trailing slash not allowed in zookeeper path");
        }
        String[] split = key.split("/");
        StringBuilder builder = new StringBuilder();
        for (String s : split) {
            builder.append(s);
            // exists(..) makes one GET request
            if (!s.isEmpty() && !exists(builder.toString())) {
                // one SET request
                zkState.set(builder.toString(), null);
            }
            builder.append("/");
        }
    }

    /**
     * Makes:
     *   1 GET request to `key`
     */
    public Boolean exists(String key) throws IOException {
        Boolean exists = true;
        Object value = zkState.get(key);
        if (value == null) {
            exists = false;
        }
        return exists;
    }
}
