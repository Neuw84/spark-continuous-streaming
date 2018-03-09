package es.aconde.structured;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.SplittableRandom;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Data generator/consumer for Apache Kafka using Twitter's bijection
 *
 * @author Angel Conde
 */
public class ProducerConsumerDemo {

    /**
     * Avro defined schema
     */
    private static final Injection<GenericRecord, byte[]> recordInjection;

    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"alarm\","
            + "\"fields\":["
            + "  { \"name\":\"machine\", \"type\":\"string\" },"
            + "  { \"name\":\"sensor\", \"type\":\"string\" },"
            + "  { \"name\":\"data\", \"type\":\"int\" },"
            + "  { \"name\":\"eventTime\", \"type\":\"long\" }"
            + "]}";
    //kafka properties
    public static final Properties KAFKA_PROPS;

    private static final Schema.Parser parser = new Schema.Parser();
    private static final Schema schema = parser.parse(USER_SCHEMA);

    //static inizialitation block 
    static {
        KAFKA_PROPS = new Properties();
        KAFKA_PROPS.put("bootstrap.servers", "localhost:9092");
        KAFKA_PROPS.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KAFKA_PROPS.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KAFKA_PROPS.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KAFKA_PROPS.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KAFKA_PROPS.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KAFKA_PROPS.put("group.id", "test");
        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    /**
     *
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        //declare de runnable for the producer
        Runnable producer = () -> {
            KafkaProducer<String, byte[]> kafkaProd = new KafkaProducer<>(KAFKA_PROPS);
            SplittableRandom random = new SplittableRandom();
            System.out.println("Starting Kafka Producer:" + Thread.currentThread().getName());
            while (true) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("machine", "machineId-" + random.nextInt(10));
                avroRecord.put("sensor", "id-" + random.nextInt(1000));
                avroRecord.put("data", random.nextInt(10000));
                avroRecord.put("eventTime", System.currentTimeMillis());
                //serialize via Twitter's bijection 
                byte[] bytes = recordInjection.apply(avroRecord);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>("out", bytes);
                kafkaProd.send(record);
                try {
                    //send a record each 100 miliseconds
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ex) {
                    System.out.println(ex.getLocalizedMessage());
                }
            }
        };
        //declare de runnable for the consumer
        Runnable consumer = () -> {
            System.out.println("Starting Kafka Consumer:" + Thread.currentThread().getName());
            KafkaConsumer<String, byte[]> kafkaCons = new KafkaConsumer<>(KAFKA_PROPS);
            List<String> topics = Arrays.asList("in");
            kafkaCons.subscribe(topics);
            SplittableRandom random = new SplittableRandom();
            while (true) {
                //we poll the kafka broker each 10 miliseconds
                ConsumerRecords<String, byte[]> records = kafkaCons.poll(5);
                for (ConsumerRecord<String, byte[]> record : records) {
                    //deserialize via Twitter's bijection
                    GenericRecord avro = recordInjection.invert(record.value()).get();
                    long receivedTime = System.currentTimeMillis();
                    long eventTime = (long) avro.get("eventTime");
                    System.out.println("Latency in ms: " + (receivedTime - eventTime) + " for record: " + avro);

                }
            }
        };
        //declare and start consumer/producer threads
        Thread prod = new Thread(producer);
        Thread cons = new Thread(consumer);
        prod.start();
        cons.start();

    }

}
