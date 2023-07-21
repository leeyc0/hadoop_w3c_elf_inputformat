package io.github.leeyc0.hadoop_w3c_elf_inputformat;

import java.util.Map;

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
 */
// We have to suppress rawtypes warning because sparkContext.newAPIHadoopFile
// does not support parameterized class (type erasure in class reflection)
@SuppressWarnings("rawtypes")
public final class W3CElfLogInputFormat extends FileInputFormat<NullWritable, Map> {

  @Override
  public RecordReader<NullWritable, Map>
    createRecordReader(final InputSplit split,
                       final TaskAttemptContext context) {
    var tabTelimiter = context.getConfiguration().get(
        "w3celfloginputformat.tabdelimiter");
    if (null != tabTelimiter && "true".equals(tabTelimiter)) {
        return new W3CElfLogReader(true);
    } else {
        return new W3CElfLogReader(false);
    }
  }

  @Override
  protected boolean isSplitable(final JobContext context, final Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

}
