package io.needles.kafka2hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class Kafka2HadoopMapper extends Mapper<IntWritable, LongWritable, Text, Text> {
	public static final String KAFKA_BROKER_CONNECTION_STRING = "kafka2hadoop.kafkaBroker";
	public static final String KAFKA_TOPIC = "kafka2hadoop.kafkaTopic";

	private static Logger logger = Logger.getLogger(Kafka2HadoopMapper.class);

	private static byte[] emptyBytes = {};
	
	@Override
	protected void map(IntWritable key, LongWritable value,
			Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String kafkaConnectionStr = conf.get(KAFKA_BROKER_CONNECTION_STRING);
		String kafkaTopic = conf.get(KAFKA_TOPIC);

		logger.info("Attempting to fetch up to " + value.get() + " for partition " + key.get());

		try (KafkaConsumer<byte[], byte[]> kafkaConsumer = Utils.kafkaConsumer(kafkaConnectionStr)){
			int partitionId = key.get();

			long latestWatermark = fetchData(kafkaConsumer, kafkaTopic, partitionId, value.get(), context);

			logger.info("Writing out new watermark of " + latestWatermark);
			Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
			Utils.writeWatermark(conf, new Path(workOutputPath, "_watermarks"), partitionId, latestWatermark);
		}
	}

    /**
     * Fetch data from the given topic/partition and return the latest watermark
     * @return
     */
	long fetchData(
	        Consumer<byte[], byte[]> consumer,
            String kafkaTopic, int partitionId, long lastOffset,
            Context context ) throws IOException, InterruptedException {

        Text outputKey = new Text();
        Text outputVal = new Text();

        TopicPartition topicPartition = new TopicPartition(kafkaTopic, partitionId);
        consumer.assign(Arrays.asList(topicPartition));

        logger.info("Seeking to topic-partition end");
        consumer.seekToEnd(Arrays.asList(topicPartition));

        // Get the latest offset(ie the seq of the latest record + 1)
        long latestOffset = consumer.position(topicPartition);
        logger.info("Found latest offset of " + latestOffset);

        // Seek to watermark(seq of the next record to fetch)
        logger.info("Seeking back to initial offset of " + lastOffset);
        consumer.seek(topicPartition, lastOffset);

        logger.info("Starting to fetch data");
        while (consumer.position(topicPartition) < latestOffset){
            ConsumerRecords<byte[], byte[]> records = consumer.poll(0);
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

        // The seq of the next record to fetch next time around
        return consumer.position(topicPartition);
    }
	
}
