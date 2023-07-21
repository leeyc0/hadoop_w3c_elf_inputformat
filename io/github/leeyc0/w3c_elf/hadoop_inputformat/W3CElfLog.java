package io.github.leeyc0.w3c_elf.hadoop_inputformat;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Class that holds log entry.
 */
public final class W3CElfLog implements Serializable {
    /**
     * datetime of the log.
     */
    private ZonedDateTime datetime;

    /**
     * Set datetime. For compliance of JavaBeans only.
     * @param datetime
     */
    public void setDatetime(final ZonedDateTime datetime) {
        this.datetime = datetime;
    }

    /**
     * @return datetime of the log. For compliance of JavaBeans only.
     */
    public ZonedDateTime getdDateTime() {
        return datetime;
    }

    /**
     * log entry. Key of is field name. Value is the parsed log field entry.
     */
    private Map<String, String> log;

    /**
     * Set log. For compliance of JavaBeans only.
     * @param log
     */
    public void setLog(final Map<String, String> log) {
        this.log = log;
    }

    /**
     * @return log entry.
     */
    public Map<String, String> getLog() {
        return log;
    }

    /**
     * Gets log field by key.
     * @param field key.
     * @return log of the specified field.
     */
    public String getLogField(final String field) {
        return log.get(field);
    }

    /**
     * Constructor. For compliance of JavaBeans only.
     */
    public W3CElfLog() {
        datetime = null;
        log = null;
    }

    /**
     * Constructor.
     * @param datetime
     * @param log
     */
    public W3CElfLog(final ZonedDateTime datetime, final Map<String, String> log) {
        this.datetime = datetime;
        this.log = log;
    }

    @Override
    public String toString() {
        return "Time: " + datetime + ", Log: " + log;
    }
}
