package io.needles.kafka2hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import static io.needles.kafka2hadoop.Kafka2HadoopMapper.*;

/**
 * Main entry point for running a kafka 2 hadoop job.
 * The args are as follows
 * 1. The kafka broker string,
 * 2. The kafka topic to fetch,
 * 3. The hadoop destination directory.
 * 4. The source watermarks directory.
 * 
 * The source watermarks directory must also be writable.
 * The new watermarks will be available in the destination/_watermarks directory
 *
 *
 * From a mapreduce point of view:
 *   The input files will be the list of input partitions.
 *   The output files will be the actual data being written
 *   and the dest watermarks will be written out at the end of each map task as a
 *   side effect file
 */
public class Main extends Configured implements Tool {
    public static final String APP = "kafka2hadoop";

	public int run(String... args) throws Exception {
	    if(args.length != 4){
	    	System.err.println(
	    			  "Expected exactly 4 arguments\n"
	    			+ "  1. The kafka broker connection string\n"
	    			+ "  2. The kafka topic to fetch\n"
	    			+ "  3. The hadoop destination directory\n"
	    			+ "  4. The source watermarks directory");
	    	return 1;
	    }
	    
	    String kafkaBrokerConnectionStr = args[0];
	    String kafkaTopicName = args[1];
	    Path destDir = new Path(args[2]);
	    Path sourceWatermarksDir = new Path(args[3]);

        Configuration conf = getConf();

        // Create partition watermarks
	    try(KafkaConsumer<byte[], byte[]> consumer = Utils.kafkaConsumer(kafkaBrokerConnectionStr)){
		    try(FileSystem fs = sourceWatermarksDir.getFileSystem(conf)){
		        createPartitionWatermarks(consumer, kafkaTopicName, fs, sourceWatermarksDir);
            }
	    }
	    
        // Remove dest dir
	    try(FileSystem fs = destDir.getFileSystem(conf)){
	    	fs.delete(destDir, true);
	    }

	    // Build and run job
	    Job job = buildJob(kafkaBrokerConnectionStr, kafkaTopicName, sourceWatermarksDir, destDir);

	    return job.waitForCompletion(true) ? 0 : 1;
	}

	protected void createPartitionWatermarks(
            KafkaConsumer<byte[], byte[]> consumer,
            String kafkaTopicName,
	        FileSystem watermarkFs,
            Path sourceWatermarksDir) throws IOException {

        // 1. Read in the watermark files.
        Map<Integer, Long> watermarks = new HashMap<>();

        IntWritable key = new IntWritable();
        LongWritable val = new LongWritable();
        RemoteIterator<LocatedFileStatus> filesIter = watermarkFs.listFiles(sourceWatermarksDir, true);
        while (filesIter.hasNext()){
            LocatedFileStatus file = filesIter.next();
            try (Reader reader = new SequenceFile.Reader(getConf(), SequenceFile.Reader.file(file.getPath()))){
                // Assume exactly one kv pair in file
                reader.next(key, val);
                watermarks.put(key.get(), val.get());
            }

        }

        // 2. Iterate over the partitions as listed by kafka and
        //    add any missing partitions to the watermarks dir
        List<PartitionInfo> partitions = consumer.partitionsFor(kafkaTopicName);
        for (PartitionInfo partition : partitions){
            int partitionId = partition.partition();
            if (!watermarks.containsKey(partitionId)){
                Utils.writeWatermark(getConf(), sourceWatermarksDir, partitionId, 0);
            }
        }
    }

    /**
     * Builds the actual Job that is to be submitted to hadoop
     * @param kafkaBrokerConnectionStr
     * @param kafkaTopicName
     * @param sourceWatermarksDir
     * @param destDir
     * @throws IOException
     */
	protected Job buildJob(
	        String kafkaBrokerConnectionStr,
            String kafkaTopicName,
            Path sourceWatermarksDir,
            Path destDir) throws IOException {

        Configuration conf = getConf();
        conf.set(KAFKA_BROKER_CONNECTION_STRING, kafkaBrokerConnectionStr, "CLI Args");
        conf.set(KAFKA_TOPIC, kafkaTopicName, "CLI Args");
        Job job = Job.getInstance(conf, APP);
        job.setJarByClass(Main.class);
        job.setMapperClass(Kafka2HadoopMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, sourceWatermarksDir);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, destDir);
        return job;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);
	}
}
