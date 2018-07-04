package com.jdxd.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class JdxdKafkaConsumer {
    private String topic = "test";
    private KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        JdxdKafkaConsumer jc = new JdxdKafkaConsumer();
        jc.init();


    }

    private void init() {
        Properties props = getConfig();
        consumer = new KafkaConsumer<String, String>(props);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    consumer.subscribe(Arrays.asList(topic));

                    ConsumerRecords<String, String> records = consumer.poll(10000);

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.topic()+"---key---"+record.key()+"--value---"+record.value());
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("------");
                }
            }
        }).start();



    }

    public Properties getConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "bigdata:9092");
        props.put("group.id", "testGroup");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
