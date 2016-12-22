package com.patterncat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.Properties;

/**
 * java -jar -Dfile.encoding=utf-8 \
 *  kafka-consumer-0.0.1-SNAPSHOT.jar \
 *  localhost:9092 topicname groupid earliest
 */
@SpringBootApplication
public class KafkaConsumerApplication {

	private static volatile boolean isStop = false;

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				isStop = true;
			}
		});
	}

	@Bean
	public CommandLineRunner consumeKafka(){
		return new CommandLineRunner() {
			@Override
			public void run(String... strings) throws Exception {
				String bootstrap = strings[0];
				String topic = strings[1];
				String group = strings[2];
				String offset = strings[3];

				Properties props = new Properties();
				props.put("bootstrap.servers", bootstrap);
				props.put("group.id", group);
				props.put("enable.auto.commit", "true");  //自动commit
				props.put("auto.commit.interval.ms", "1000"); //定时commit的周期
				props.put("session.timeout.ms", "30000"); //consumer活性超时时间
				props.put("auto.offset.reset",offset);
				props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				props.put("key.deserializer.encoding", "UTF8");
				props.put("value.deserializer.encoding", "UTF8");
				KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
				consumer.subscribe(Arrays.asList(topic));
				while (!isStop) {
					ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
					for (ConsumerRecord<String, String> record : records){
						try{
							System.out.println(record);
						}catch (Exception e){
							e.printStackTrace();
						}
					}
				}
			}
		};
	}
}
