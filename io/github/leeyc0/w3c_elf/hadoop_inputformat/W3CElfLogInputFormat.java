package io.github.leeyc0.w3c_elf.hadoop_inputformat;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An InputFormat for W3C ELF files.
 * https://www.w3.org/TR/WD-logfile.html
 * configuration properties:
 * w3celfloginputformat.tabdelimiter (boolean): Set to true if input file is using tab as seperator.
 *   Defaults to be false.
 * w3celfloginputformat.timezone: Timezone of the log file. See java.time.ZoneId.of for the string
 *   to be set. If unset then the timezone will be java.time.ZoneOffset.UTC.
 * w3celfloginputformat.withdatefield (boolean): Set to true to indicate input file has date field.
 *   The primary usage of this option is to indicate the file may be splittable if set to true.
 *   (Depends on compression codec.) Defaults to be true.
 *   If set to false, then the date of the log entry would have to be inferred from Date directive.
 *   In this case the input file would not be splittable because it is impossible to look up the
 *   previous Date directive from a file split. (The Date directive can be in the middle of the file.)
 */
public final class W3CElfLogInputFormat extends FileInputFormat<NullWritable, W3CElfLog> {

    @Override
    public RecordReader<NullWritable, W3CElfLog>
    createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        final var tabSeperator = context.getConfiguration().get(
            "w3celfloginputformat.tabseperator");
        
        ZoneId timezone = ZoneOffset.UTC;
        final var timezoneProperty = context.getConfiguration().get(
            "w3celfloginputformat.timezone");
        if (null != timezoneProperty) {
            timezone = ZoneId.of(timezoneProperty);
        }

        final boolean withDateField = isDateFieldSet(context);
        if (null != tabSeperator && "true".equalsIgnoreCase(tabSeperator)) {
            return new W3CElfLogReader(true, withDateField, timezone);
        } else {
            return new W3CElfLogReader(false, withDateField, timezone);
        }
    }

    /**
      * W3C file is unsplittable if #Date directive is required
      * to build log entry, because it can be appear at anywhere.
      * It is impossible to look up where the previous
      * #Date directive appears in the middle of the file.
      */
    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        if (isDateFieldSet(context)) {
            final CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
            if (null == codec) {
                return true;
            }
            return codec instanceof SplittableCompressionCodec;
        } else {
            return false;
        }
    }

    private boolean isDateFieldSet(JobContext context) {
        final var dateFieldSet = context.getConfiguration().get(
            "w3celfloginputformat.withdatefield");
        if (null != dateFieldSet && "true".equalsIgnoreCase(dateFieldSet)) {
            return true;
        } else {
            return false;
        }
    }
}
