package kafka2hadoop;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Utils {
	static KafkaConsumer<byte[], byte[]> kafkaConsumer(String connectionString){
		 Properties props = new Properties();
		 props.put("bootstrap.servers", connectionString);
		 props.put("group.id", "kafka2hadoop");
		 props.put("enable.auto.commit", "false");
		 props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		 props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		 KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
		 return consumer;
	}
	
	static void writeWatermark(Configuration conf, Path dir, int partitionId, long watermark) throws IOException{
		IntWritable key = new IntWritable();
		LongWritable val = new LongWritable();

		try (Writer writer = SequenceFile.createWriter(conf,
				SequenceFile.Writer.file(new Path(dir, Integer.toString(partitionId))),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(LongWritable.class))){
			key.set(partitionId);
			val.set(watermark);
			writer.append(key, val);
			writer.hflush();
		}
	}
}
