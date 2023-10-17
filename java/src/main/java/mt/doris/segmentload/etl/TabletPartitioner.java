package mt.doris.segmentload.etl;

import mt.doris.segmentload.etl.EtlJobConfig.EtlPartition;
import mt.doris.segmentload.etl.EtlJobConfig.EtlPartitionInfo;
import org.apache.spark.Partitioner;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class TabletPartitioner extends Partitioner {
    private int numPartition;
    private EtlJobConfig.EtlPartitionInfo partitionInfo;

    private Map<Long, Integer> partitionId2StartOffset = new HashMap<>();

    public TabletPartitioner(EtlPartitionInfo partitionInfo) {
        int totalTabletNum = 0;
        this.partitionInfo = partitionInfo;
        for (EtlPartition etlPartition : partitionInfo.partitions) {
            partitionId2StartOffset.put(etlPartition.partitionId, totalTabletNum);
            totalTabletNum += etlPartition.bucketNum;
        }
        numPartition = totalTabletNum;
    }

    @Override
    public int numPartitions() {
        return numPartition ;
    }

    @Override
    public int getPartition(Object key) {
        Tuple2<Long, Integer> tupleKey = (Tuple2<Long, Integer>) key;
        if (!partitionId2StartOffset.containsKey(tupleKey._1())) {
            throw new RuntimeException("unknow partition id: " + tupleKey._1());
        }
        return partitionId2StartOffset.get(tupleKey._1()) + tupleKey._2();
    }
}
