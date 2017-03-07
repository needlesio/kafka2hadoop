package kafka2hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class Kafka2HadoopMapper extends Mapper<IntWritable, LongWritable, Text, Text> {
	private Logger logger = Logger.getLogger(Kafka2HadoopMapper.class);

	private static byte[] emptyBytes = {};
	
	@Override
	protected void map(IntWritable key, LongWritable value,
			Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String kafkaConnectionStr = conf.get("kafka2hadoop.kafkaBroker");
		String kafkaTopic = conf.get("kafka2hadoop.kafkaTopic");
		
		Text outputKey = new Text();
		Text outputVal = new Text();

		logger.info("Attempting to fetch up to " + value.get() + " for partition " + key.get());

		try (KafkaConsumer<byte[], byte[]> kafkaConsumer = Utils.kafkaConsumer(kafkaConnectionStr)){
			int partitionId = key.get();
			TopicPartition topicPartition = new TopicPartition(kafkaTopic, partitionId);
			kafkaConsumer.assign(Arrays.asList(topicPartition));
			logger.info("Seeking to topic-partition end");
			kafkaConsumer.seekToEnd(Arrays.asList(topicPartition));
			// Get the most current offset
			long latestOffset = kafkaConsumer.position(topicPartition);
			logger.info("Found latest offset of " + latestOffset);
			
			// Seek to watermark
			logger.info("Seeking back to initial offset of " + value.get());
			kafkaConsumer.seek(topicPartition, value.get());

			logger.info("Starting to fetch data");
			while (kafkaConsumer.position(topicPartition) < latestOffset){
				ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(0);
				for (ConsumerRecord<byte[], byte[]> record : records){
					if (record.key() != null){
						outputKey.set(record.key(), 0, record.key().length);
					}else {
						outputKey.set(emptyBytes, 0, 0);
					}
					
					outputVal.set(record.value(), 0, record.value().length);
					context.write(outputKey, outputVal);
				}

				// Stop hadoop from killing the map task
				context.progress();
			}
			
			// write out new watermark as side effect file.
			long latestWatermark = kafkaConsumer.position(topicPartition);
			logger.info("Writing out new watermark of " + latestWatermark);
			Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
			Utils.writeWatermark(conf, new Path(workOutputPath, "_watermarks"), partitionId, latestWatermark);
		}
	}
	
}
