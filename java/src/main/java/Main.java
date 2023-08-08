import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.StructType;

import java.io.File;

public class Main {



    public static String getHiveDataSQL() {
        String sql = "SELECT `aor_id`, `location_id`, `union_id`, `dt`, `log_type`\n" +
                "\t, `partition_view`, `first_city_id`, `second_city_id`, `pinhf_batch_id`, `pinhf_biz_type_id`\n" +
                "\t, `abtest`, `aor_type`, `app_name`, `app_version`, `biz_type_id`\n" +
                "\t, `business_category_key`, `category`, `city_id`, `client_id`, `client_ip`\n" +
                "\t, `device_model`, `device_type`, `download_channel`, `dpid`, `event_id`\n" +
                "\t, `event_timestamp`, `event_type`, `geo_city_id`, `geo_city_name`, `geo_district_name`\n" +
                "\t, `geohash`, `idfa`, `imei`, `is_auto`, `is_native`\n" +
                "\t, `item_id`, `item_index`, `latitude`, `launch_channel`, `locate_city_id`\n" +
                "\t, `log_channel`, `login_type`, `longitude`, `op_id`, `order_flow_bridge_id`\n" +
                "\t, `order_system_id`, `os`, `os_version`, `page_city_id`, `page_city_name`\n" +
                "\t, `page_id`, `page_name`, `page_stay_time`, `poi_id`, `push_id`\n" +
                "\t, `refer_page_id`, `refer_request_id`, `refer_url`, `reg_latitude`, `reg_longitude`\n" +
                "\t, `request_id`, `sdk_version`, `sec_app_version`, `second_business_category`, `sequence`\n" +
                "\t, `session_id`, `sku_id`, `source_info`, `spu_id`, `stat_time`\n" +
                "\t, `stid`, `tag`, `unified_device_model`, `url`, `user_agent`\n" +
                "\t, `user_id`, `utm_campaign`, `utm_content`, `utm_medium`, `utm_source`\n" +
                "\t, `utm_term`, `uuid`, `wifi_status`, `first_channel_id`, `second_channel_id`\n" +
                "\t, `first_channel_name`, `second_channel_name`, `third_channel_id`, `third_channel_name`, `pinhf_user_type_id`\n" +
                "\t, `pinhf_user_type_name`, `first_city_name`, `second_city_name`, `third_city_name`, `wechat_channel_id`\n" +
                "\t, `open_gid`, `group_id` from mart_waimai.aggr_flow_pinhf_info WHERE dt >= '20230807' and dt < '20230808' and 1 = 1 and 1=1";
        return sql;
    }




    public static void main(String[] args) throws InterruptedException {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("nativeBulkLoad")
                .enableHiveSupport()
                .getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        String parquetDstPath = "./parquet/tmp/";
        Dataset<Row> dataset = sparkSession.sql(getHiveDataSQL());
        dataset = dataset.repartition(800, dataset.col("union_id"));
        StructType schema = dataset.schema();
        dataset.foreachPartition( partition -> {
            int partitionId = TaskContext.getPartitionId();
            String parquetFileName = String.format("test_%d.parquet", partitionId);
            File file = new File(parquetFileName);
            System.out.println("file path: " + file.getAbsolutePath());
            ParquetWriteSupport parquetWriteSupport = new ParquetWriteSupport();
            Configuration conf = new Configuration();
            conf.setBoolean("spark.sql.parquet.writeLegacyFormat", false);
            conf.setBoolean("spark.sql.parquet.int64AsTimestampMillis", false);
            conf.setBoolean("spark.sql.parquet.int96AsTimestamp", true);
            conf.setBoolean("spark.sql.parquet.binaryAsString", false);
            conf.set("spark.sql.parquet.outputTimestampType", "INT96");
            parquetWriteSupport.setSchema(schema, conf);
            System.out.println(schema);
            System.out.println(parquetWriteSupport == null);
            file.deleteOnExit();
            ParquetWriter<InternalRow> parquetWriter = new ParquetWriter<InternalRow>(new Path("file://" + file.getAbsolutePath()), parquetWriteSupport,
                    CompressionCodecName.SNAPPY, 256 * 1024 * 1024, 16 * 1024, 1024 * 1024,
                    false, false,
                    ParquetProperties.WriterVersion.PARQUET_1_0, conf);
            System.out.println(parquetWriter == null);
            ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
            while (partition.hasNext()) {
                // 写入本地 Parquet 文件
                Row row = partition.next();
                InternalRow internalRow = encoder.toRow(row);
//                for (int i = 0; i < schema.length(); i++) {
//                    internalRow.update(i, row.get(i));
//                }
                parquetWriter.write(internalRow);
            }
            parquetWriter.close();
            JniExample jni = new JniExample();
            jni.segmentBuild("test_tablet_meta", file.getParent());

        });
        // sleep 0.5 hour
        Thread.sleep(1800*1000);
        sparkContext.stop();
    }
}
