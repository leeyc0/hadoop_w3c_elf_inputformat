package io.github.leeyc0.w3c_elf.hadoop_inputformat;

import java.io.InputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;

import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;

public final class W3CElfLogReader extends RecordReader<NullWritable, W3CElfLog> {
    /**
     * true if the log file has Date field
     */
    private final boolean withDateField;

    /**
     * timezone
     */
    private final ZoneId timezone;

    /**
     * csvParser to parse the log.
     */
    private RFC4180Parser csvParser;

    /**
     * The underlying LineRecordReader to actually read the ELF log file.
     */
    private LineRecordReader reader;

    /**
     * W3C ELF log version.
     */
    private String version = null;

    /**
     * A list storing field names.
     */
    private String[] fields = null;

    /**
     * return fields
     */
    public String[] getFields() {
        return fields;
    }

    /**
     * Date directive. Use this if there is no date field
     */
    private LocalDateTime dateDirective = null;

    /**
     * TextInputFormat to create LineRecordReader for header parsing.
     */
    private static final TextInputFormat TEXT_INPUT_FORMAT = new TextInputFormat();

    /**
     * time string formatter
     */
    private static final DateTimeFormatter DATE_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);

    /**
     * time string formatter
     */
    private static final DateTimeFormatter TIME_FORMATTER =
        DateTimeFormatter.ofPattern("HH:mm[:ss[.S]]", Locale.ENGLISH);

    /**
     * HashMap to cache datetime
     */
    private HashMap<String, ZonedDateTime> dateTimeCache = new HashMap<String, ZonedDateTime>();

    /**
     * Constructor.
     * @param tabSeperator If true, use tab as seperator, otherwise space.
     * @param withDateField If true, the reader assumes the log file is with Date field.
     *   If set to false, the file is always unsplittable.
     * @param timezone Timezone of the log file. The log file itself does not contain
     *   timezone information and need to be specified using this parameter.
     */
    public W3CElfLogReader(final boolean tabSeperator, final boolean withDateField,
    final ZoneId timezone) {
        super();
        csvParser = new RFC4180ParserBuilder()
            .withSeparator(tabSeperator ? '\t' : ' ')
            .build();
        this.withDateField = withDateField;
        this.timezone = timezone;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    /**
     * Key is unused for this reader, always returns NullWritable.
     */
    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    /**
     * Returns parsed logs.
     * @return {@link W3CElfLog} entry.
     *   date and time fields are not contained in {@link W3CElfLog.getLog}. Instead they are
     *   parsed and contained in {@link W3CElfLog.getDatetime}.
     *   If the log cannot be parsed, the unparsed log will be stored in W3CElfLog.getLog with
     *   key "_error", and W3CElfLog.getDatetime will be null.
     */
    @Override
    public W3CElfLog getCurrentValue() throws IOException {
        final var str = reader.getCurrentValue().toString();
        final var parsedLine = csvParser.parseLine(str);
        final var map = new HashMap<String, String>();
        ZonedDateTime datetime = null;

        try {
            if (parsedLine != null && parsedLine.length == fields.length) {
                for (var i = 0; i < fields.length; i++) {
                    map.put(fields[i], parsedLine[i]);
                }
                final var date = map.get("date");
                final var time = map.get("time");
                map.remove("date");
                map.remove("time");
                datetime = parseDateTime(date, time);
                return new W3CElfLog(datetime, map);
            }
        } catch (DateTimeParseException e) {
            // no-op
        }

        map.clear();
        map.put("_error", str);
        return new W3CElfLog(null, map);
    }

    /**
     * @return getProgress() of underlying LineRecordReader.
     */
    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    // TODO: implement this
    public void parseHeader(Path path, Configuration conf) throws IOException {
        FSDataInputStream file = null;
        InputStream in = null;
        LineReader reader = null;
        final var line = new Text();
        try {
            final var fs = path.getFileSystem(conf);
            final var codec = new CompressionCodecFactory(conf).getCodec(path);
            file = fs.open(path);
            if (codec == null) {
                in = file;
            } else {
                final var decompressor = CodecPool.getDecompressor(codec);
                in = codec.createInputStream(file, decompressor);
            }
            reader = new LineReader(in, conf);
            while (true) {
                final var readBytes = reader.readLine(line);
                if (readBytes == 0) {
                    break;
                }
                if (line.getLength() > 0 && line.charAt(0) != '#') {
                    break;
                }
                final var directive = line.toString().split(": +");
                if (directive.length == 2) {
                    switch (directive[0]) {
                        case "#Version":
                            version = directive[1];
                            break;
                        case "#Fields":
                            fields = directive[1].split("\\s");
                            break;
                        case "#Date":
                            dateDirective = parseDateDirective(directive[1]);
                            break;
                    }
                }
            }
        } finally {
            if (reader != null) {
                reader.close();
            } else if (in != null) {
                in.close();
            } else if (file != null) {
                file.close();
            }
        }
    }

    /**
     * Reads log file directive lines. Only Version, Fields and Date are recoginzed, other directives are ignored.
     * Currently Version must be 1.0. This seems to be the only version defined and exists in the wild.
     * @param genericSplit
     * @param context
     * @throws IOException, W3CElfLogFormatException
     */
    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException, W3CElfLogFormatException {
        // we need to create a no-split reader to read whole file, to parse the directives
        // Ensuring no-split is required to prevent the possiblity of file splitting at the directive lines
        final var split = (FileSplit) genericSplit;
        final var job = context.getConfiguration();
        final var file = split.getPath();

        // parse the directive headers
        parseHeader(file, job);

        if (!"1.0".equals(version)) {
            throw new W3CElfLogFormatException("Missing or unsupported log file version");
        }

        if (fields == null || fields.length == 0) {
            throw new W3CElfLogFormatException("Missing field definition");
        }

        if (!Arrays.stream(fields).anyMatch("time"::equals)) {
            throw new W3CElfLogFormatException("Missing require field \"time\"");
        }

        if (withDateField && !Arrays.stream(fields).anyMatch("date"::equals)) {
            throw new W3CElfLogFormatException("withdatefield is set but \"date\" field is missing");
        }

        if (!Arrays.stream(fields).anyMatch("date"::equals) && dateDirective == null) {
            throw new W3CElfLogFormatException("Both #Date directive and \"date\" field are missing");
        }

        // parsing directive finished, create the actual record reader.
        reader = (LineRecordReader) TEXT_INPUT_FORMAT.createRecordReader(split, context);
        reader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, W3CElfLogFormatException {
        var nextKeyValue = reader.nextKeyValue();
        // skip the directive header lines if present
        while (nextKeyValue) {
            final var line = reader.getCurrentValue();
            if (line.getLength() > 0 && line.toString().charAt(0) == '#') {
                final var directive = line.toString().split(": +");
                if (directive.length == 2 && "#Date".equals(directive[0])) {
                    dateDirective = parseDateDirective(directive[1]);
                }
                nextKeyValue = reader.nextKeyValue();
            } else {
                break;
            }
        }
        return nextKeyValue;
    }

    /**
     * Parse datetime.
     * @param date date string. If date is null then date will be inferred from dateDirective.
     * @param time time string.
     * @return parsed datetime.
     */
    private ZonedDateTime parseDateTime(final String dateStr, final String timeStr) {
        final var cacheStr = new StringBuilder()
            .append(dateStr).append(" ").append(timeStr).toString();
        var datetime = dateTimeCache.get(cacheStr);
        if (datetime == null) {
            LocalDate date = null;
            final var time = LocalTime.parse(timeStr, TIME_FORMATTER);
            // infer date from Date directive if dateStr is not provided
            if (dateStr == null) {
                date = dateDirective.toLocalDate();
                final var dateDirectiveTime = dateDirective.toLocalTime();
                // increment the date if we detect current time is over midnight
                if (time.compareTo(dateDirectiveTime) < 0) {
                    date = date.plusDays(1);
                }
            } else {
                date = LocalDate.parse(dateStr, DATE_FORMATTER);
            }
            datetime = LocalDateTime.of(date, time).atZone(timezone);
            dateTimeCache.put(cacheStr, datetime);
        }
        return datetime;
    }

    /**
     * Parse datetime.
     * @param datetimeStr Combined datetimeStr. Used in Date directive.
     * @return parsed datetime.
     * @throws W3CElfLogFormatException
     */
    private LocalDateTime parseDateDirective(final String datetimeStr) throws W3CElfLogFormatException {
        try {
            dateTimeCache.clear();
            final var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm[:ss[.S]]", Locale.ENGLISH);
            return LocalDateTime.parse(datetimeStr, formatter);
        } catch (DateTimeParseException e) {
            throw new W3CElfLogFormatException("Unable to parse Date directive: " + datetimeStr, e);
        }
    }
}
