import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
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

    private static void readProcessOutput(Process process) {
        read(process.getInputStream(), System.out);
        read(process.getErrorStream(), System.out);
    }
    private static void read(InputStream inputStream, PrintStream out) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while((line = reader.readLine()) != null) {
                out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }



    public static void main(String[] args) throws InterruptedException, IOException {
        SparkSession sparkSession = SparkSession.builder().appName("nativeBulkLoad").enableHiveSupport().getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        String parquetDstPath = "/ghnn01/kylin/sparkload/parquet_test/";
        int TARGET_FILE_SIZE = 524288000;
        Dataset<Row> dataset = sparkSession.sql(getHiveDataSQL());
        dataset = dataset.cache();
        Path path = new Path(parquetDstPath);
        Configuration conf = sparkContext.hadoopConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] files = fs.listStatus(path, new PathFilter() {
            public boolean accept(Path path) {
                return !path.getName().startsWith("_") && !path.getName().startsWith(".");
            }
        });
        List<String> paths = (List) Arrays.stream(files).filter((f) -> {
            return f.getLen() != 0L;
        }).map((f) -> {
            return f.getPath().toString();
        }).collect(Collectors.toList());
        int parallel = paths.size();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> rdd = jsc.parallelize(paths, parallel);
        Configuration hadoopConfiguration = jsc.hadoopConfiguration();
        Broadcast<SerializableWritable<Configuration>> broadcastedHadoopConfig = jsc.broadcast(new SerializableWritable(hadoopConfiguration));
        rdd.foreachPartition((partition) -> {
            List<String> fileList = new ArrayList();

            while(partition.hasNext()) {
                fileList.add(partition.next());
            }

            FileSystem hadoopFS = (new Path(parquetDstPath)).getFileSystem((Configuration)((SerializableWritable)broadcastedHadoopConfig.value()).value());
            int stageId = TaskContext.get().stageId();
            long taskId = TaskContext.get().taskAttemptId();
            int attemptId = TaskContext.get().attemptNumber();
            int fileIdx = 0;
            List<String> targetFilePaths = new ArrayList();

            for (String outputFilePath : fileList) {
                String localPath = String.format("qarquet_dir/%d/%d/%d/%d/test_parquet.parquet", stageId, taskId, attemptId, fileIdx);
                File file = new File(localPath);
                file.deleteOnExit();
                hadoopFS.copyToLocalFile(new Path(outputFilePath), new Path(file.getAbsolutePath()));
                String parentDir = file.getParentFile().getAbsolutePath();
                LOG.info("hdfs file dir:{}, local file dir:{}, local parent dir:{}, local file size:{}", new Object[]{outputFilePath, file.getAbsolutePath(), parentDir, file.getTotalSpace()});
                ++fileIdx;
                List<String> cmds = Arrays.asList("./segment_builder", "--meta_file=./test_tablet_meta", String.format("--data_path=%s", parentDir));
                ProcessBuilder cmdProcess = new ProcessBuilder(cmds);
                Process process = cmdProcess.start();
                readProcessOutput(process);
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new RuntimeException("error when exec cmd:" + cmds);
                }

                String outputTarFile = String.format("%s/segment_%d_%d_%d_%d.tar.gz", parentDir, stageId, taskId, attemptId, fileIdx);
                String hdfsoutputTarFile = String.format("/ghnn01/kylin/sparkload/segment_test/segment_%d_%d_%d_%d.tar.gz", stageId, taskId, attemptId, fileIdx);
                FileIOUtils.packFilesToTarGz(parentDir + "/segment/", outputTarFile);
                hadoopFS.copyFromLocalFile(new Path(outputTarFile), new Path(hdfsoutputTarFile));
                targetFilePaths.add(hdfsoutputTarFile);
            }

            for (String targetFilePath : targetFilePaths) {
                System.out.println(targetFilePath);
            }
        });
        Thread.sleep(1800000L);
        sparkContext.stop();
    }
}
