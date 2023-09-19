package mt.doris.segmentload.common;

import com.google.common.base.Strings;

public class SegmentLoadException extends Exception {
    public SegmentLoadException(String msg, Throwable cause) {
        super(Strings.nullToEmpty(msg), cause);
    }

    public SegmentLoadException(Throwable cause) {
        super(cause);
    }

    public SegmentLoadException(String msg, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(Strings.nullToEmpty(msg), cause, enableSuppression, writableStackTrace);
    }

    public SegmentLoadException(String msg) {
        super(Strings.nullToEmpty(msg));
    }
}


