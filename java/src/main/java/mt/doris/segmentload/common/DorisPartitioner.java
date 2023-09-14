package mt.doris.segmentload.common;

import mt.doris.segmentload.etl.EtlJobConfig;
import org.apache.spark.Partitioner;
import java.io.Serializable;
import java.util.List;

public class DorisPartitioner extends Partitioner {
    private static final String UNPARTITIONED_TYPE = "UNPARTITIONED";
    private EtlJobConfig.EtlPartitionInfo partitionInfo;
    private List<PartitionKeys> partitionKeys;
    List<Integer> partitionKeyIndexes;
    public DorisPartitioner(EtlJobConfig.EtlPartitionInfo partitionInfo,
                            List<Integer> partitionKeyIndexes,
                            List<PartitionKeys> partitionKeys) {
        this.partitionInfo = partitionInfo;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.partitionKeys = partitionKeys;
    }

    public int numPartitions() {
        if (partitionInfo == null) {
            return 0;
        }
        if (partitionInfo.partitionType.equalsIgnoreCase(UNPARTITIONED_TYPE)) {
            return 1;
        }
        return partitionInfo.partitions.size();
    }

    public int getPartition(Object var1) {
        if (partitionInfo.partitionType != null
                && partitionInfo.partitionType.equalsIgnoreCase(UNPARTITIONED_TYPE)) {
            return 0;
        }
        DorisColumns key = (DorisColumns)var1;
        // get the partition columns from key as partition key
        DorisColumns partitionKey = new DorisColumns(key, partitionKeyIndexes);
        // TODO: optimize this by use binary search
        for (int i = 0; i < partitionKeys.size(); ++i) {
            if (partitionKeys.get(i).isRowInPartitoin(partitionKey)) {
                return i;
            }
        }
        return -1;
    }

    public static class PartitionKeys implements Serializable {
        public boolean isMaxPartition;
        // for range partitions
        public DorisColumns startKeys;
        public DorisColumns endKeys;
        public List<DorisColumns> listPartitionKeys;

        public boolean isRowInPartitoin(DorisColumns row) {
            return (listPartitionKeys == null || listPartitionKeys.size() == 0) ?
                    isRowInRange(row) : isRowInList(row);
        }

        public boolean isRowInRange(DorisColumns row) {
            if (isMaxPartition) {
                return startKeys.compareTo(row) <= 0;
            } else {
                return startKeys.compareTo(row) <= 0 && endKeys.compareTo(row) > 0;
            }
        }
        public boolean isRowInList(DorisColumns row) {
            for (DorisColumns cols : listPartitionKeys) {
                if (cols.compareTo(row) == 0) {
                    return true;
                }
            }
            return false;
        }

        public String toString() {
            if (listPartitionKeys == null || listPartitionKeys.size() == 0) {
                return "PartitionRangeKey{" +
                        "isMaxPartition=" + isMaxPartition +
                        ", startKeys=" + startKeys +
                        ", endKeys=" + endKeys +
                        '}';
            } else {
                return "PartitionRangeKey{" +
                        "listPartitionKeys=" + listPartitionKeys +
                        '}';
            }
        }
    }
}