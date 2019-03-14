package com.demo.hadoop.hbase;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

public class HBaseRDD<T> extends RDD<T> {

    private static <T> ClassTag<T> createClassTag(Class<T> klass) {
        return scala.reflect.ClassTag$.MODULE$.apply(klass);
    }

    private final HBasePartitioner partitioner;
    private final String tableName;
    private final byte[] startKey;
    private final byte[] stopKey;
    private final Function<Result, T> mapper;

    public HBaseRDD(SparkContext sparkContext,
                    Class<T> klass,
                    HBasePartitioner partitioner,
                    String tableName,
                    byte[] startKey,
                    byte[] stopKey,
                    Function<Result, T> mapper) {
        super(new EmptyRDD<>(sparkContext, createClassTag(klass)), createClassTag(klass));
        this.partitioner = partitioner;
        this.tableName = tableName;
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.mapper = mapper;
    }


    @Override
    public Seq<String> getPreferredLocations(Partition split) {
        Set<String> locations = ImmutableSet.of(((HBasePartition) split).getRegionHostname());
        return JavaConversions.asScalaSet(locations).toSeq();
    }

    @Override
    public Iterator<T> compute(Partition split, TaskContext context) {
        HBasePartition partition = (HBasePartition) split;
        try (Connection connection = ConnectionFactory.createConnection()) {
            Scan scan = new Scan();
            scan.setStartRow(partition.getStart())
                    .setStopRow(partition.getStop())
                    .setCacheBlocks(false);
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner scanner = table.getScanner(scan);
            return JavaConversions.asScalaIterator(
                    StreamSupport.stream(scanner.spliterator(), false).map(mapper).iterator()
            );
        } catch (IOException e) {
            throw new RuntimeException("Region scan failed", e);
        }
    }

    @Override
    public Partition[] getPartitions() {
        return partitioner.getPartitions(Bytes.toBytes(tableName), startKey, stopKey);
    }


}
