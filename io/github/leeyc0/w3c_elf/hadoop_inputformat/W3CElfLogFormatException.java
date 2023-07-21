package io.github.leeyc0.w3c_elf.hadoop_inputformat;

/**
 * This has to be a unchecked exception, otherwise we cannot throw them in relevant
 * overrided Reader methods.
 */
public class W3CElfLogFormatException extends RuntimeException {
    public W3CElfLogFormatException() {
        super();
    }

    public W3CElfLogFormatException(String message) {
        super(message);
    }

    public W3CElfLogFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    public W3CElfLogFormatException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public W3CElfLogFormatException(Throwable cause) {
        super(cause);
    }
}
