package io.github.leeyc0.hadoop_w3c_elf_inputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.ForeachFunction;
import scala.Tuple2;

public final class Test {
    private Test() {
    }

    /**
     * main function.
     * @param args
     */
    public static void main(final String[] args) {
        final var spark = SparkSession
            .builder()
            .getOrCreate();
        final var sc = spark.sparkContext();
        var rdd = sc.newAPIHadoopFile(args[0], W3CElfLogInputFormat.class,
            NullWritable.class, W3CElfLog.class, sc.hadoopConfiguration());
        spark.createDataset(rdd, Encoders.tuple(Encoders.kryo(NullWritable.class), Encoders.kryo(W3CElfLog.class)))
            .foreach((ForeachFunction<Tuple2<NullWritable, W3CElfLog>>)tuple -> System.out.println(tuple._2()));
    }
}
