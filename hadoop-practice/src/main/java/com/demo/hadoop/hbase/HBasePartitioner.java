package com.demo.hadoop.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Partition;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class HBasePartitioner implements Serializable {

    public Partition[] getPartitions(byte[] table, byte[] start, byte[] stop) {
        try (RegionLocator regionLocator = ConnectionFactory.createConnection().getRegionLocator(TableName.valueOf(table))) {
            List<HRegionLocation> regionLocations = regionLocator.getAllRegionLocations();
            int regionCount = regionLocations.size();
            List<Partition> partitions = Lists.newArrayListWithExpectedSize(regionCount);
            int partition = 0;
            for (HRegionLocation regionLocation : regionLocations) {
                HRegionInfo regionInfo = regionLocation.getRegionInfo();
                byte[] regionStart = regionInfo.getStartKey();
                byte[] regionStop = regionInfo.getEndKey();
                if (!skipRegion(start, stop, regionStart, regionStop)) {
                    partitions.add(new HBasePartition(regionLocation.getHostname(),
                            partition++,
                            max(start, regionStart),
                            min(stop, regionStop)));
                }
            }
            return partitions.toArray(new Partition[partition]);
        } catch (IOException e) {
            throw new RuntimeException("Could not create HBase region partitions", e);
        }
    }

    private static boolean skipRegion(byte[] scanStart, byte[] scanStop, byte[] regionStart, byte[] regionStop) {
        // check scan starts before region stops, and that the scan stops before the region starts
        return min(scanStart, regionStop) == regionStop || max(scanStop, regionStart) == regionStart;
    }

    private static byte[] min(byte[] left, byte[] right) {
        if (left.length == 0) {
            return left;
        }
        if (right.length == 0) {
            return right;
        }
        return Bytes.compareTo(left, right) < 0 ? left : right;
    }
    private static byte[] max(byte[] left, byte[] right) {
        if (left.length == 0) {
            return right;
        }
        if (right.length == 0) {
            return left;
        }
        return Bytes.compareTo(left, right) >= 0 ? left : right;
    }
}

