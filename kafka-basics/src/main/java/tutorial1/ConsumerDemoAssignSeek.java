package tutorial1;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        TopicPartition PartitionToReadFrom = new TopicPartition(topic,0);
        long positionToReadFrom = 15L;
        consumer.assign(Collections.singletonList(PartitionToReadFrom));
        consumer.seek(PartitionToReadFrom, positionToReadFrom);
        int NumOfMsgToRead = 5;
        int NumOfMessageRead = 0;
        boolean keepReading = true;
        while(keepReading){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record :records ){
                NumOfMessageRead += 1;
                logger.info("Key: " + record.key() + ", Value " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                if (NumOfMessageRead >= NumOfMsgToRead){
                    keepReading = false;
                    break;
                }
                logger.info("Exiting the application.");
            }
        }
    }
}
