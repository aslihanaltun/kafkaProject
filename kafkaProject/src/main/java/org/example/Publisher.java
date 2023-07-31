package org.example;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class Publisher {

    public static void main(String[] args) {
        String topicName = "SUBSCRIBER";
        String bootstrapServers = "localhost:9092";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        while (true) {
            int subscId = random.nextInt(1000) + 1;
            String subscName = "John";
            String subscSurname = "Doe";
            String msisdn = "1234567890";

            String message = "SUBSC_ID=" + subscId + ",SUBSC_NAME=" + subscName + ",SUBSC_SURNAME=" + subscSurname + ",MSISDN=" + msisdn;

            producer.send(new ProducerRecord<>(topicName, message), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error publishing message: " + exception.getMessage());
                } else {
                    System.out.println("Published message: " + message);
                }
            });

            try {
                Thread.sleep(1000); // Wait for 1 second before sending the next message
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
