package io.github.leeyc0.hadoop_w3c_elf_inputformat;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.sql.SparkSession;

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
        sc.newAPIHadoopFile(args[0], W3CElfLogInputFormat.class,
            NullWritable.class, Map.class, sc.hadoopConfiguration())
            .toJavaRDD()
            .foreach(tuple -> System.out.println(tuple._2()));
    }
}
