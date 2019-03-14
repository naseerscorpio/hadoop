package com.demo.hadoop.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.IOException;

public class HBaseDataframeWriter {

    private final String table;
    private final String columnFamily;
    private final String keyField;

    public HBaseDataframeWriter(String table, String columnFamily, String keyField) {
        this.table = table;
        this.columnFamily = columnFamily;
        this.keyField = keyField;
    }


    public void write(Dataset<Row> payload) throws IOException {

        Job newAPIJobConfiguration = Job.getInstance(HBaseConfiguration.create());
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = payload.javaRDD().mapToPair((PairFunction<Row, ImmutableBytesWritable, Put>) row -> {
            Put put = new Put(Bytes.toBytes(row.getString(row.fieldIndex(this.keyField))));
            for (String field : row.schema().fieldNames()) {
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(field),
                        Bytes.toBytes(row.getString(row.fieldIndex(field))));
            }
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
    }

}
