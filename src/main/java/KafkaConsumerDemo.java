import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class KafkaConsumerDemo {

	public static String BootstrapServers;
	public static String GroupId;
	public static String TopicName;

	public static void loadConfig() {
		  Config config = ConfigFactory.load("application.conf").getConfig("com.alibaba-inc.cwchan");
		  Config kafkaConfig = config.getConfig("Kafka");
		  KafkaConsumerDemo.BootstrapServers = kafkaConfig.getString("bootstrapServers");
		  KafkaConsumerDemo.GroupId = kafkaConfig.getString("groupId");
		  KafkaConsumerDemo.TopicName = kafkaConfig.getString("topicName");
	}

	public static void main(String[] args) {

		KafkaConsumerDemo.loadConfig();
		 
		Properties props = new Properties();
		// 設置接入點，請通過控制台獲取對應Topic的接入點
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);

		// 可更加實際拉去數據和客戶的版本等設置此值，默認30s
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
		// 每次poll的最大數量
		// 注意該值不要改得太大，如果poll太多數據，而不能在下次poll之前消費完，則會觸發一次負載均衡，產生卡頓
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
		// 消息的反序列化方式
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		// 當前消費實例所屬的消費組，請在控制台申請之後輸入
		// 屬於同一個組的消費實例，會負載消費消息
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GroupId);
		// 構造消息對象，也即生成一個消費實例
		KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(
				props);
		// 設置消費組訂閱的Topic，可以訂閱多個
		// 如果GROUP_ID_CONFIG是一樣，則訂閱的Topic也建議設置成一樣
		List<String> subscribedTopics = new ArrayList<String>();
		// 如果需要訂閱多個Topic，則在這裡add進去即可
		// 每個Topic需要先在控制台進行創建
		subscribedTopics.add(TopicName);
		consumer.subscribe(subscribedTopics);

		// 循環消費消息
		while (true) {
			try {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				// 必須在下次poll之前消費完這些數據, 且總耗時不得超過SESSION_TIMEOUT_MS_CONFIG
				// 建議開一個單獨的線程池來消費消息，然後異步返回結果
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(
							String.format("Consume partition:%d offset:%d", record.partition(), record.offset()));
					System.out.println(String.format("key:%s\nvalue:%s", record.key(), record.value()));
				}
			} catch (Exception e) {
				try {
					Thread.sleep(1000);
				} catch (Throwable ignore) {

				}
				// 參考常見報錯:
				// https://help.aliyun.com/document_detail/68168.html?spm=a2c4g.11186623.6.567.2OMgCB
				e.printStackTrace();
			}
		}
	}
}