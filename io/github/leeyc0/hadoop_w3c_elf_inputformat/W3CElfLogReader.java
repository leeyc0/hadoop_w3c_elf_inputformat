package io.github.leeyc0.hadoop_w3c_elf_inputformat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

// We have to suppress rawtypes warning because sparkContext.newAPIHadoopFile
// does not support parameterized class
@SuppressWarnings("rawtypes")
public final class W3CElfLogReader extends RecordReader<NullWritable, Map> {
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
     * TextInputFormat to create LineRecordReader for header parsing.
     */
    private static final TextInputFormat TEXT_INPUT_FORMAT = new TextInputFormat();

    /**
     * A list storing field names.
     */
    private String[] fields = null;

    /**
     * Constructor.
     * @param tabSeperator If true, use tab as seperator, otherwise space.
     */
    public W3CElfLogReader(final boolean tabSeperator) {
        super();
        csvParser = new RFC4180ParserBuilder()
            .withSeparator(tabSeperator ? '\t' : ' ')
            .build();
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
     * @return A map storing parsed log. Map key is field name. If the log cannot be parsed,
     *   the unparsed log will be stored in the map with key "_error".
     */
    @Override
    public Map<String, String> getCurrentValue() throws IOException {
        var str = reader.getCurrentValue().toString();
        var parsedLine = csvParser.parseLine(str);
        var map = new HashMap<String, String>();
        if (parsedLine != null && parsedLine.length == fields.length) {
            for (var i = 0; i < fields.length; i++) {
                map.put(fields[i], parsedLine[i]);
            }
        } else {
            map.put("_error", str);
        }
        return map;
    }

    /**
     * @return getProgress() of underlying LineRecordReader.
     */
    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    /**
     * Reads log file directive lines. Only Version and Fields are recoginzed, other directives are ignored.
     * Currently Version must be 1.0. This seems to be the only version defined and exists in the wild.
     * After calling this method, call {@link getFields()} to obtain the fields.
     */
    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        // we need to create a no-split reader to read whole file, to parse the directives
        // Ensuring no-split is required to prevent the possiblity of file splitting at the directive lines
        final var split = (FileSplit) genericSplit;
        final var job = context.getConfiguration();
        final var file = split.getPath();
        final var fileSystem = file.getFileSystem(job);
        final var fileSize = fileSystem.getFileStatus(file).getLen();
        final var blockLocation = fileSystem.getFileBlockLocations(file, 0, fileSize);
        final var blockLocationSet = new HashSet<String>();

        for (var b : blockLocation) {
            blockLocationSet.addAll(Arrays.asList(b.getHosts()));
        }
        var fileBlockLocation = blockLocationSet.toArray(new String[0]);

        final var splitForHeader = new FileSplit(file, 0, fileSize, fileBlockLocation);
        final var readerForHeader = (LineRecordReader) TEXT_INPUT_FORMAT.createRecordReader(splitForHeader, context);
        readerForHeader.initialize(splitForHeader, context);

        try {
            // parse the directive headers
            while (readerForHeader.nextKeyValue()) {
                var line = readerForHeader.getCurrentValue();
                if (line.getLength() > 0 && line.charAt(0) != '#') {
                    break;
                }
                var directive = line.toString().split(": *");
                if (directive.length == 2) {
                    if (directive[0].equals("#Version")) {
                        version = directive[1];
                    } else if (directive[0].equals("#Fields")) {
                        fields = directive[1].split("\\s");
                    }
                }
            }

            if (!"1.0".equals(version)) {
                throw new IOException("Missing or unsupported log file version");
            }

            if (fields == null || fields.length == 0) {
                throw new IOException("Missing field definition");
            }

            // parsing directive finished, create the actual record reader.
            reader = (LineRecordReader) TEXT_INPUT_FORMAT.createRecordReader(split, context);
            reader.initialize(split, context);
        } finally {
            readerForHeader.close();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        var nextKeyValue = reader.nextKeyValue();
        // skip the directive header lines if present
        while (nextKeyValue) {
            var line = reader.getCurrentValue();
            if (line.getLength() > 0 && line.toString().charAt(0) == '#') {
                nextKeyValue = reader.nextKeyValue();
            } else {
                break;
            }
        }
        return nextKeyValue;
    }
}
