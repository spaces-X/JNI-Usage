package mt.doris.segmentload.etl;

import java.io.*;
import java.math.BigInteger;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import mt.doris.segmentload.common.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.Partitioner;
import org.apache.spark.SerializableWritable;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SegmentLoadETL implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentLoadETL.class);
    private static final String NULL_FLAG = "\\N";
    private static final String BITMAP_TYPE = "bitmap";
    private static final String DPP_RESULT_FILE = "dpp_result.json";
    private static final String PARTITION_ID = "__partitionIdx__";
    private static final String BUCKET_ID = "__bucketIdx__";
    private static final String ETL_OUTPUT_FILE_NAME_DESC_V1 = "version.label.tableId.partitionId.bucket.schemaHash.parquet";
    // tableId.partitionId.indexId.bucket.schemaHash
    public static final String TABLET_META_FORMAT = "%d.%d.%d.%d";


    private SparkSession spark;

    private EtlJobConfig etlJobConfig;

    private String hiveDB;

    private String hiveTable;

    private String sourceFilter;

    private List<String> dorisColumns;

    private Dataset<Row> datasetFromHive;

    private SerializableConfiguration serializableHadoopConf;

    private String jobConfigPath;

    public static String getTabletMetaStr(String filePath, long indexId) throws Exception {
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String[] fileNameArr = fileName.split("\\.");
        // check file version
        switch (EtlJobConfig.FilePatternVersion.valueOf(fileNameArr[0])) {
            case V1:
                // version.label.tableId.partitionId.indexId.bucket.schemaHash.parquet
                if (fileNameArr.length != ETL_OUTPUT_FILE_NAME_DESC_V1.split("\\.").length) {
                    throw new Exception("etl output file name error, format: " + ETL_OUTPUT_FILE_NAME_DESC_V1
                            + ", name: " + fileName);
                }
                long tableId = Long.parseLong(fileNameArr[2]);
                long partitionId = Long.parseLong(fileNameArr[3]);
                int bucket = Integer.parseInt(fileNameArr[4]);
                int schemaHash = Integer.parseInt(fileNameArr[5]);
                // tableId.partitionId.indexId.bucket.schemaHash
                return String.format(TABLET_META_FORMAT, tableId, partitionId, indexId, bucket, schemaHash);
            default:
                throw new Exception("etl output file version error. version: " + fileNameArr[0]);
        }
    }

    public String generateExtractSQL() {
        String sql = String.format("select %s from `%s`.`%s` where %s", String.join(",", dorisColumns), hiveDB, hiveTable, sourceFilter);
        return sql;
    }



    public void setEtlJobConfig(EtlJobConfig etlJobConfig) {
        this.etlJobConfig = etlJobConfig;
    }


    public SegmentLoadETL(SparkSession spark, String jobConfigPath, String hiveDB, String hiveTable, String sourceFilter, List<String>dorisColumns) {
        this.spark = spark;
        this.jobConfigPath = jobConfigPath;
        this.hiveDB = hiveDB;
        this.hiveTable = hiveTable;
        this.sourceFilter = sourceFilter;
        this.dorisColumns = dorisColumns;
        this.datasetFromHive = null;
    }

    public SegmentLoadETL(SparkSession spark, String jobConfigPath, Dataset<Row> datasetFromHive) {
        this.spark = spark;
        this.jobConfigPath = jobConfigPath;
        this.datasetFromHive = datasetFromHive;
    }

    public void init() {
        serializableHadoopConf = new SerializableConfiguration(spark.sparkContext().hadoopConfiguration());
        initConfig(spark, jobConfigPath);
    }

    private void initConfig(SparkSession spark, String jobConfigFilePath) {
        LOG.info("job config file path: " + jobConfigFilePath);
        Dataset<String> ds = spark.read().textFile(jobConfigFilePath);
        String jsonConfig = ds.first();
        LOG.info("rdd read json config: " + jsonConfig);
        etlJobConfig = EtlJobConfig.configFromJson(jsonConfig);
//        etlJobConfig.outputPath = etlJobConfig.outputPath.replace(MT_HDFS_PREFIX,"");
        LOG.info("etl job config: " + etlJobConfig);
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

    private List<DorisPartitioner.PartitionKeys> createDorisPartitionKeys(
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws SegmentLoadException {
        List<DorisPartitioner.PartitionKeys> partitionKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitionInfo.partitions) {
            DorisPartitioner.PartitionKeys keysOfOnePartition = new DorisPartitioner.PartitionKeys();
            if (partitionInfo.partitionType.equalsIgnoreCase("RANGE")) {
                List<Object> startKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.startKeys.size(); i++) {
                    Object value = partition.startKeys.get(i);
                    startKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                keysOfOnePartition.startKeys = new DorisColumns(startKeyColumns);
                if (!partition.isMaxPartition) {
                    keysOfOnePartition.isMaxPartition = false;
                    List<Object> endKeyColumns = new ArrayList<>();
                    for (int i = 0; i < partition.endKeys.size(); i++) {
                        Object value = partition.endKeys.get(i);
                        endKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                    }
                    keysOfOnePartition.endKeys = new DorisColumns(endKeyColumns);
                } else {
                    keysOfOnePartition.isMaxPartition = true;
                }
            } else if (partitionInfo.partitionType.equalsIgnoreCase("LIST")) {
                List<DorisColumns> keyColumns = Lists.newArrayList();
                for (int i = 0; i < partition.listKeys.size(); i++) {
                    List<Object> elementOfKeys = partition.listKeys.get(i);
                    List<Object> elementOfKeyCols = Lists.newArrayList();
                    for (int j = 0; j < elementOfKeys.size(); j++) {
                        Object value = elementOfKeys.get(j);
                        elementOfKeyCols.add(convertPartitionKey(value, partitionKeySchema.get(j)));
                    }
                    keyColumns.add(new DorisColumns(elementOfKeyCols));
                }
                keysOfOnePartition.listPartitionKeys = keyColumns;


            } else {
                throw new RuntimeException("unknown partition type: " + partitionInfo.partitionType);
            }
            partitionKeys.add(keysOfOnePartition);
        }
        return partitionKeys;
    }

    private Dataset<Row> convertSrcDataframeToDstDataframe(EtlJobConfig.EtlIndex baseIndex,
                                                           Dataset<Row> srcDataframe,
                                                           StructType dstTableSchema,
                                                           EtlJobConfig.EtlFileGroup fileGroup) throws SegmentLoadException {

        Dataset<Row> dataframe = srcDataframe;
        StructType srcSchema = dataframe.schema();
        Set<String> srcColumnNames = new HashSet<>();
        for (StructField field : srcSchema.fields()) {
            srcColumnNames.add(field.name());
        }
        Map<String, EtlJobConfig.EtlColumnMapping> columnMappings = fileGroup.columnMappings;
        // 1. process simple columns
        Set<String> mappingColumns = null;
        if (columnMappings != null) {
            mappingColumns = columnMappings.keySet();
        }
        List<String> dstColumnNames = new ArrayList<>();
        for (StructField dstField : dstTableSchema.fields()) {
            dstColumnNames.add(dstField.name());
            EtlJobConfig.EtlColumn column = baseIndex.getColumn(dstField.name());
            if (!srcColumnNames.contains(dstField.name())) {
                if (mappingColumns != null && mappingColumns.contains(dstField.name())) {
                    // mapping columns will be processed in next step
                    continue;
                }
                if (column.defaultValue != null) {
                    if (column.defaultValue.equals(NULL_FLAG)) {
                        dataframe = dataframe.withColumn(dstField.name(), functions.lit(null));
                    } else {
                        dataframe = dataframe.withColumn(dstField.name(), functions.lit(column.defaultValue));
                    }
                } else if (column.isAllowNull) {
                    dataframe = dataframe.withColumn(dstField.name(), functions.lit(null));
                } else {
                    throw new SegmentLoadException("Reason: no data for column:" + dstField.name());
                }
            }
            if (column.columnType.equalsIgnoreCase("DATE")) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(DataTypes.DateType));
            } else if (column.columnType.equalsIgnoreCase("DATETIME")) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(DataTypes.TimestampType));
            } else if (column.columnType.equalsIgnoreCase("BOOLEAN")) {
                dataframe = dataframe.withColumn(dstField.name(),
                        functions.when(functions.lower(dataframe.col(dstField.name())).equalTo("true"), "1")
                                .when(dataframe.col(dstField.name()).equalTo("1"), "1")
                                .otherwise("0"));
            } else if (!column.columnType.equalsIgnoreCase(BITMAP_TYPE) && !dstField.dataType().equals(DataTypes.StringType)) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(dstField.dataType()));
            } else if (column.columnType.equalsIgnoreCase(BITMAP_TYPE) && dstField.dataType().equals(DataTypes.BinaryType)) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(DataTypes.BinaryType));
            } else if (column.columnType.equalsIgnoreCase("LARGEINT")) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(DataTypes.StringType));
            } else {
//                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(dstField.dataType()));
            }
            if (fileGroup.isNegative && !column.isKey) {
                // negative load
                // value will be convert te -1 * value
                dataframe = dataframe.withColumn(dstField.name(), functions.expr("-1 *" + dstField.name()));
            }
        }
        // 2. process the mapping columns
        for (String mappingColumn : mappingColumns) {
            String mappingDescription = columnMappings.get(mappingColumn).toDescription();
            if (mappingDescription.toLowerCase().contains("hll_hash")) {
                continue;
            }
            // here should cast data type to dst column type
            // what should do here？
            dataframe = dataframe.withColumn(mappingColumn,
                    functions.expr(mappingDescription).cast(dstTableSchema.apply(mappingColumn).dataType()));
        }
        return dataframe;

    }

    private Object convertPartitionKey(Object srcValue, Class dstClass) throws SegmentLoadException {
        if (dstClass.equals(Float.class) || dstClass.equals(Double.class)) {
            return null;
        }
        if (srcValue instanceof Double) {
            if (dstClass.equals(Short.class)) {
                return ((Double) srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return ((Double) srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
                return ((Double) srcValue).longValue();
            } else if (dstClass.equals(BigInteger.class)) {
                // TODO(wb) gson will cast origin value to double by default
                // when the partition column is largeint, this will cause error data
                // need fix it thoroughly
                return new BigInteger(((Double) srcValue).toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                double srcValueDouble = (double)srcValue;
                return convertToJavaDate((int) srcValueDouble);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                double srcValueDouble = (double)srcValue;
                return convertToJavaDatetime((long)srcValueDouble);
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else {
            LOG.warn("unsupport partition key:" + srcValue);
            throw new SegmentLoadException("unsupport partition key:" + srcValue);
        }
    }

    private java.sql.Timestamp convertToJavaDatetime(long src) {
        String dateTimeStr = Long.valueOf(src).toString();
        if (dateTimeStr.length() != 14) {
            throw new RuntimeException("invalid input date format for SparkDpp");
        }

        String year = dateTimeStr.substring(0, 4);
        String month = dateTimeStr.substring(4, 6);
        String day = dateTimeStr.substring(6, 8);
        String hour = dateTimeStr.substring(8, 10);
        String min = dateTimeStr.substring(10, 12);
        String sec = dateTimeStr.substring(12, 14);

        return java.sql.Timestamp.valueOf(String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, min, sec));
    }

    private java.sql.Date convertToJavaDate(int originDate) {
        int day = originDate & 0x1f;
        originDate >>= 5;
        int month = originDate & 0x0f;
        originDate >>= 4;
        int year = originDate;
        return java.sql.Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
    }

    private Dataset repartitionAsTablet(Dataset dataframe, EtlJobConfig.EtlTable etlTable) throws SegmentLoadException {

        EtlJobConfig.EtlIndex baseIndex = null;
        for (EtlJobConfig.EtlIndex indexMeta : etlTable.indexes) {
            if (indexMeta.isBaseIndex) {
                baseIndex = indexMeta;
                break;
            }
        }
        for (EtlJobConfig.EtlIndex indexMeta : etlTable.indexes) {
            if (indexMeta.isBaseIndex) {
                baseIndex = indexMeta;
                break;
            }
        }

        // get key column names and value column names seperately
        List<String> keyAndPartitionColumnNames = new ArrayList<>();
        List<String> valueColumnNames = new ArrayList<>();
        for (EtlJobConfig.EtlColumn etlColumn : baseIndex.columns) {
            if (etlColumn.isKey) {
                keyAndPartitionColumnNames.add(etlColumn.columnName);
            } else {
                valueColumnNames.add(etlColumn.columnName);
            }
        }

        EtlJobConfig.EtlPartitionInfo partitionInfo = etlTable.partitionInfo;
        List<Integer> partitionKeyIndex = new ArrayList<Integer>();
        List<Class> partitionKeySchema = new ArrayList<>();
        for (String key : partitionInfo.partitionColumnRefs) {
            for (int i = 0; i < baseIndex.columns.size(); ++i) {
                EtlJobConfig.EtlColumn column = baseIndex.columns.get(i);
                if (column.columnName.equals(key)) {
                    partitionKeyIndex.add(keyAndPartitionColumnNames.indexOf(key));
                    partitionKeySchema.add(DppUtils.getClassFromColumn(column));
                    break;
                }
            }
        }
        List<DorisPartitioner.PartitionKeys> partitionKeys = createDorisPartitionKeys(partitionInfo, partitionKeySchema);

        Partitioner partitioner = new DorisPartitioner(etlTable.partitionInfo, partitionKeyIndex, partitionKeys);
        StructType schemaWithPartitionIdx = dataframe.schema().add(PARTITION_ID, DataTypes.LongType)
                .add(BUCKET_ID, DataTypes.IntegerType);
        ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(schemaWithPartitionIdx);
        dataframe = dataframe.map((MapFunction<Row, Row>) row -> {
            List<Object> keyColumns = new ArrayList<>();
            for (int i = 0; i < keyAndPartitionColumnNames.size(); i++) {
                String columnName = keyAndPartitionColumnNames.get(i);
                Object columnObject = row.get(row.fieldIndex(columnName));
                keyColumns.add(columnObject);
            }
            DorisColumns key = new DorisColumns(keyColumns);
            int pid = partitioner.getPartition(key);
            long hashValue = DppUtils.getHashValue(row, partitionInfo.distributionColumnRefs, row.schema());
            long paritionId = partitionInfo.partitions.get(pid).partitionId;
            int bucketId = (int) ((hashValue & 0xffffffff) % partitionInfo.partitions.get(pid).bucketNum);
            List<Object> columns = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                columns.add(row.get(i));
            }
            columns.add(paritionId);
            columns.add(bucketId);
            Row toReturn = new GenericRow(columns.toArray());
            return toReturn;
        }, rowEncoder);

        Dataset<Row> repartitioned = dataframe.repartition(2000, dataframe.col(PARTITION_ID), dataframe.col(BUCKET_ID));
        repartitioned = repartitioned.sortWithinPartitions(dataframe.col(PARTITION_ID), dataframe.col(BUCKET_ID));
        return repartitioned;
    }

    private void write2ParquetFile(Dataset<Row> tablets, Map<Long, EtlJobConfig.EtlTable> tables) {
        for (Long tableId: tables.keySet()) {
            EtlJobConfig.EtlTable dorisETLTable = tables.get(tableId);
            EtlJobConfig.EtlIndex baseIdx = dorisETLTable.indexes.stream().filter(x -> x.isBaseIndex).findFirst().get();
            StructType targetSchema = DppUtils.createDstTableSchema(baseIdx.columns, false, true);
            ExpressionEncoder.Serializer<Row> serializer = RowEncoder.apply(targetSchema).createSerializer();
            System.out.println("dataframe schema is:" + tablets.schema());
            System.out.println("target schema is:" + targetSchema);

            tablets.foreachPartition(
                    new ForeachPartitionFunction<Row>() {
                        @Override
                        public void call(Iterator<Row> iterator) throws Exception {
                            Configuration conf = new Configuration(serializableHadoopConf.value());
                            FileSystem fs = FileSystem.get(URI.create(etlJobConfig.outputPath), conf);
                            TaskContext taskContext = TaskContext.get();
                            long taskPartitionId = taskContext.partitionId();
                            int attempNum = taskContext.attemptNumber();
                            Long lastPartitionId = null;
                            Integer lastBuckeId = null;
                            ParquetWriter<InternalRow> parquetWriter = null;
                            String tmpPath = "";
                            String dstPath = "";

                            while (iterator.hasNext()) {
                                Row row = iterator.next();
                                Long partitionIdInRow = row.getAs(PARTITION_ID);
                                Integer bucketIdInRow = row.getAs(BUCKET_ID);


                                List<Object> tupleRow = new ArrayList<>();
                                for (int i = 0; i < row.size()-2; i++) {
                                    // remove bucket id and partition id in row.
                                    tupleRow.add(row.get(i));
                                }
                                if (partitionIdInRow == null || bucketIdInRow == null) {
                                    System.out.println("partition id or bucket id is null, row content is:" + row.toString());
                                    throw new RuntimeException("partition id or bucket id is null, maybe caused by OrthogonalBitmapEncoding");
                                }

                                if (parquetWriter == null || !partitionIdInRow.equals(lastPartitionId) || !bucketIdInRow.equals(lastBuckeId)) {
                                    if (parquetWriter != null) {
                                        parquetWriter.close();
                                        try {
                                            fs.rename(new Path(tmpPath), new Path(dstPath));
                                        } catch (IOException e) {
                                            LOG.warn("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath + " failed. exception:" + e);
                                            throw e;
                                        }
                                    }
                                    dstPath = String.format(etlJobConfig.outputPath + "/parquet/" + etlJobConfig.outputFilePattern,
                                            tableId, partitionIdInRow, bucketIdInRow, baseIdx.schemaHash);
                                    tmpPath = dstPath + "/tmp/" + String.format("%d.%d.tmp", taskPartitionId, attempNum);
                                    conf.setBoolean("spark.sql.parquet.writeLegacyFormat", false);
                                    conf.setBoolean("spark.sql.parquet.int64AsTimestampMillis", false);
                                    conf.setBoolean("spark.sql.parquet.int96AsTimestamp", true);
                                    conf.setBoolean("spark.sql.parquet.binaryAsString", false);
                                    conf.set("spark.sql.parquet.outputTimestampType", "INT96");
                                    ParquetWriteSupport.setSchema(targetSchema, conf);
                                    ParquetWriteSupport parquetWriteSupport = new ParquetWriteSupport();
                                    CompressionCodecName compressName = CompressionCodecName.UNCOMPRESSED;
                                    if (Boolean.parseBoolean(etlJobConfig.customizedProperties.getOrDefault("custom.parquet.compress", "false"))) {
                                        compressName = CompressionCodecName.SNAPPY;
                                    }
                                    parquetWriter = new ParquetWriter<InternalRow>(new Path(tmpPath), parquetWriteSupport,
                                            compressName, 256 * 1024 * 1024, 16 * 1024, 1024 * 1024,
                                            false, false,
                                            ParquetProperties.WriterVersion.PARQUET_1_0, conf);
                                    LOG.info("[HdfsOperate]>> initialize writer succeed! path: " + tmpPath);
                                    lastPartitionId = partitionIdInRow;
                                    lastBuckeId = bucketIdInRow;

                                }
                                parquetWriter.write(serializer.apply(RowFactory.create(tupleRow.toArray())));
                            }
                            if (parquetWriter != null) {
                                parquetWriter.close();
                                try {
                                    fs.rename(new Path(tmpPath), new Path(dstPath));
                                    LOG.info("succeed to rename from tmpPath: {} to dstPath: {}", tmpPath, dstPath);
                                } catch (IOException ioe) {
                                    LOG.warn("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath + " failed. exception:" + ioe);
                                    throw ioe;
                                }
                            }

                        }
                    }
            );
        }
    }


    private void buildSegments() throws IOException {
        String parquetBaseDstPath = etlJobConfig.outputPath + "/parquet/";
        Path path = new Path(parquetBaseDstPath);
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] tabletFiles = fs.listStatus(path, new PathFilter() {
            public boolean accept(Path path) {
                return !path.getName().startsWith("_") && !path.getName().startsWith(".") && !path.getName().endsWith(".tmp");
            }
        });


        List<String> tabletPaths = (List) Arrays.stream(tabletFiles).filter((f) -> {
            return f.getLen() != 0L;
        }).map((f) -> {
            return f.getPath().toString();
        }).collect(Collectors.toList());
        int parallel = tabletPaths.size();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rdd = jsc.parallelize(tabletPaths, parallel);
        Configuration hadoopConfiguration = jsc.hadoopConfiguration();
        Broadcast<SerializableWritable<Configuration>> broadcastedHadoopConfig = jsc.broadcast(new SerializableWritable(hadoopConfiguration));
        List<EtlJobConfig.EtlIndex> indexes = etlJobConfig.tables.values().stream().findFirst().get().indexes;
        rdd.foreachPartition((partition) -> {
            List<String> fileList = new ArrayList();

            while(partition.hasNext()) {
                fileList.add(partition.next());
            }

            FileSystem hadoopFS = (new Path(parquetBaseDstPath)).getFileSystem((Configuration)((SerializableWritable)broadcastedHadoopConfig.value()).value());
            int stageId = TaskContext.get().stageId();
            long taskId = TaskContext.get().taskAttemptId();
            int attemptId = TaskContext.get().attemptNumber();
            int fileIdx = 0;
            List<String> targetFilePaths = new ArrayList();
            for (EtlJobConfig.EtlIndex index : indexes) {
                for (String parquetFilePath : fileList) {
                    String metaFromFileName = getTabletMetaStr(parquetFilePath, index.indexId);
                    String localPath = String.format("parquet_dir/%d/%d/%d/%d/%s/tablet.parquet", stageId, taskId, attemptId, fileIdx, metaFromFileName);
                    File file = new File(localPath);
                    file.deleteOnExit();
                    hadoopFS.copyToLocalFile(new Path(parquetFilePath), new Path(file.getAbsolutePath()));
                    String parentDir = file.getParentFile().getAbsolutePath();
                    LOG.info("hdfs file dir:{}, local file dir:{}, local parent dir:{}, local file size:{}", parquetFilePath, file.getAbsolutePath(), parentDir, file.getTotalSpace());
                    ++fileIdx;
                    List<String> cmds = Arrays.asList("./segment_builder", "--meta_file=./test_tablet_meta", String.format("--data_path=%s", parentDir));
                    ProcessBuilder cmdProcess = new ProcessBuilder(cmds);
                    Process process = cmdProcess.start();
                    readProcessOutput(process);
                    int exitCode = process.waitFor();
                    if (exitCode != 0) {
                        throw new RuntimeException("error when exec cmd:" + cmds);
                    }

                    File segmentDir = new File(parentDir + "/segment/");
                    File[] segmentFiles = segmentDir.listFiles();
                    String hdfsoutputTmpDir = etlJobConfig.outputPath + "/segment_tmp/" + metaFromFileName + "/" + String.format("tmp_%s_%s/", taskId, attemptId);
                    for (File segmentFile : segmentFiles) {
                        hadoopFS.copyFromLocalFile(new Path(segmentFile.getPath()), new Path(hdfsoutputTmpDir));
                    }
                    String hdfsoutputDir = etlJobConfig.outputPath + "/segment/" + metaFromFileName + "/";
                    hadoopFS.rename(new Path(hdfsoutputTmpDir), new Path(hdfsoutputDir));

                    targetFilePaths.add(hdfsoutputDir);
                }
            }

            for (String targetFilePath : targetFilePaths) {
                System.out.println(targetFilePath);
            }
        });
    }

    public void process() throws SegmentLoadException, IOException {
        if (datasetFromHive == null) {
            datasetFromHive = spark.sql(generateExtractSQL());
        }
        init();
        EtlJobConfig.EtlTable etlTable = etlJobConfig.tables.values().stream().findFirst().get();
        EtlJobConfig.EtlIndex baseIndex = etlTable.indexes.stream().filter(idx -> idx.isBaseIndex).findFirst().get();
        StructType dstSchema = DppUtils.createDstTableSchema(baseIndex.columns, false, true);
        convertSrcDataframeToDstDataframe(baseIndex, datasetFromHive,dstSchema, etlTable.fileGroups.get(0));
        Dataset<Row> repartitioned = repartitionAsTablet(datasetFromHive, etlTable);
        write2ParquetFile(repartitioned, etlJobConfig.tables);
        buildSegments();
    }

}
