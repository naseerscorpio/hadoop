package com.demo.hadoop.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseDataframeWriter {

    static final Logger LOGGER = Logger.getLogger(HBaseDataframeWriter.class);

    private final String table;
    private final String columnFamily;
    private final String keyField;

    public HBaseDataframeWriter(String table, String columnFamily, String keyField) {
        this.table = table;
        this.columnFamily = columnFamily;
        this.keyField = keyField;
    }


    public void write(Dataset<Row> payload) throws IOException {
        payload.printSchema();

        LOGGER.info("Writing to Hbase table " + table + " with key - " + this.keyField);

        Job newAPIJobConfiguration = Job.getInstance(HBaseConfiguration.create());
        newAPIJobConfiguration.getConfiguration().set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com:2181");
       // newAPIJobConfiguration.getConfiguration().set("hbase.master","sandbox-hdp.hortonworks.com:16000");
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        String cf = this.columnFamily;
        String kf = this.keyField;
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = payload.javaRDD().mapToPair((PairFunction<Row, ImmutableBytesWritable, Put>) row -> {
            Map<String, DataType> dtMap = createDataTypeMap(row);
            Put put = new Put(Bytes.toBytes(row.getString(row.fieldIndex(kf))));
            try {
                for (String field : row.schema().fieldNames()) {
                    String value = getValue(row, field, dtMap);
                    if(value!=null) {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(field),
                                Bytes.toBytes(value));
                    }
                }
            } catch (IllegalArgumentException ex) {
                LOGGER.error("Field not found!");
            }
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
        LOGGER.info("Successfully written to hbase!");
    }

    private static Map<String, DataType> createDataTypeMap(Row row) {
        Map<String, DataType> map = new HashMap<>();
        for (StructField field : row.schema().fields()) {
            map.put(field.name(), field.dataType());
        }
        return map;
    }

    private static String getValue(Row row, String field, Map<String, DataType> dataTypeMap) {
        if (DataTypes.IntegerType.equals(dataTypeMap.get(field))) {
            return String.valueOf(row.getInt(row.fieldIndex(field)));
        } else if (DataTypes.LongType.equals(dataTypeMap.get(field))) {
            return String.valueOf(row.getLong(row.fieldIndex(field)));
        }
        return row.getString(row.fieldIndex(field));
    }

}
