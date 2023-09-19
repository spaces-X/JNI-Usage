package mt.doris.segmentload.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;

import mt.doris.segmentload.common.SegmentLoadException;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    public static String getHiveDataSQL() {
        String sql = "SELECT `aor_id`, `location_id`, `union_id`, cast(`dt` as int) as `dt`, `log_type`\n" +
                "\t, `partition_view`, `first_city_id`, `second_city_id`, `pinhf_batch_id`, `pinhf_biz_type_id`\n" +
                "\t, `abtest`, `aor_type`, `app_name`, `app_version`, cast(`attribute` as string) as `attribute`, `biz_type_id`\n" +
                "\t, `business_category_key`, `category`, `city_id`, `client_id`, `client_ip`, cast(`custom` as string) as `custom`\n" +
                "\t, `device_model`, `device_type`, `download_channel`, `dpid`, `event_id`\n" +
                "\t, `event_timestamp`, `event_type`, cast(`extension` as string) as `extension`, `geo_city_id`, `geo_city_name`, `geo_district_name`\n" +
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
                "\t, `open_gid`, `group_id` from mart_waimai.aggr_flow_pinhf_info WHERE dt >= '20230917' and dt < '20230918' and 1 = 1 and 1=1";
        return sql;
    }


    public static void main(String[] args) throws IOException, SegmentLoadException {
        SparkSession sparkSession = SparkSession.builder().appName("nativeBulkLoad").enableHiveSupport().getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        String parquetDstPath = "/ghnn01/kylin/sparkload/parquet_test/";
        Dataset<Row> data = sparkSession.sql(getHiveDataSQL());
        String etlJobConfPath = "/ghnn01/kylin/segmentload/jobs/default_cluster_doris_vec/doris_vec__hmart_waimai_aggr_flow_pinhf_info__1695021629__cantor1365754592__0/453942011/config/jobconfig.json";
        SegmentLoadETL etl = new SegmentLoadETL(sparkSession, etlJobConfPath, data);
        etl.process();
    }
}
