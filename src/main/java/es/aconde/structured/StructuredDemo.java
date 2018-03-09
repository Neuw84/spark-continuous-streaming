package es.aconde.structured;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Structured streaming demo using Avro'ed Kafka topic as input
 *
 * @author Angel Conde
 */
public class StructuredDemo {

    private static Injection<GenericRecord, byte[]> recordInjection;
    private static StructType type;
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"alarm\","
            + "\"fields\":["
            + "  { \"name\":\"machine\", \"type\":\"string\" },"
            + "  { \"name\":\"sensor\", \"type\":\"string\" },"
            + "  { \"name\":\"data\", \"type\":\"int\" },"
            + "  { \"name\":\"eventTime\", \"type\":\"long\" }"
            + "]}";
    private static Schema.Parser parser = new Schema.Parser();
    private static Schema schema = parser.parse(USER_SCHEMA);

    static { //once per VM, lazily
        recordInjection = GenericAvroCodecs.toBinary(schema);
        type = (StructType) SchemaConverters.toSqlType(schema).dataType();

    }

    public static void main(String[] args) throws StreamingQueryException {
        //set log4j programmatically
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        //configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("kafka-structured")
                .setMaster("local[*]");

        //initialize spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //reduce task number
        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");

        //data stream from kafka
        Dataset<Row> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "out")
                .option("auto.offset.reset", "latest")
                .option("failOnDataLoss", "false")
                .option("startingOffsets", "earliest")
                .load();

        //register udf for deserializing from avro
        sparkSession.udf().register("deserialize", (byte[] data) -> {
            GenericRecord record = recordInjection.invert(data).get();
            return RowFactory.create(record.get("machine").toString(), record.get("sensor").toString(), record.get("data"), record.get("eventTime"));
        }, DataTypes.createStructType(type.fields()));

        ds1.printSchema();
        Dataset<byte[]> ds2 = ds1
                .select("value").as(Encoders.BINARY())
                .selectExpr("deserialize(value) as rows")
                .select("rows.*")
                .selectExpr("machine", "sensor", "(data*2) as processedData", "eventTime") //calculate sensor data * 2
                .map((row) -> { //this could be done via udf too?¿
                    GenericData.Record avroRecord = new GenericData.Record(schema);
                    avroRecord.put("machine", row.getString(0));
                    avroRecord.put("sensor", row.getString(1));
                    avroRecord.put("data", row.getInt(2));
                    avroRecord.put("eventTime", row.getLong(3));
                    //serialize via Twitter's bijection 
                    byte[] bytes = recordInjection.apply(avroRecord);
                    return bytes;
                }, Encoders.BINARY());

        StreamingQuery query1 = ds2
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "in")
                .option("checkpointLocation", "checkpoint")
                .outputMode("append")
                .trigger(Trigger.Continuous("1 second")) //continous mode with 1 second checkpointing
                .start();

        query1.awaitTermination();

    }
}
