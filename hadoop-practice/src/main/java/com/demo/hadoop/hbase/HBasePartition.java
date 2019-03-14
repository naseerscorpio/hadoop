package com.demo.hadoop.hbase;

import org.apache.spark.Partition;

public class HBasePartition implements Partition {

    private final String regionHostname;
    private final int partitionIndex;
    private final byte[] start;
    private final byte[] stop;

    public HBasePartition(String regionHostname, int partitionIndex, byte[] start, byte[] stop) {
        this.regionHostname = regionHostname;
        this.partitionIndex = partitionIndex;
        this.start = start;
        this.stop = stop;
    }

    public String getRegionHostname() {
        return regionHostname;
    }

    public byte[] getStart() {
        return start;
    }

    public byte[] getStop() {
        return stop;
    }

    @Override
    public int index() {
        return partitionIndex;
    }
}
