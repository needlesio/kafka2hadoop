package kafka2hadoop;

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
 */
public class Main extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
	    if(args.length != 4){
	    	System.err.println(
	    			  "Expected exactly 5 arguments\n"
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
	    
	    try(KafkaConsumer<byte[], byte[]> consumer = Utils.kafkaConsumer(kafkaBrokerConnectionStr)){
		    // Ensure that all partitions have watermark files.
	
		    try(FileSystem fs = sourceWatermarksDir.getFileSystem(conf)){
			    // 1. Read in the watermark files.
			    Map<Integer, Long> watermarks = new HashMap<>();
	
				IntWritable key = new IntWritable();
				LongWritable val = new LongWritable();
		    	RemoteIterator<LocatedFileStatus> filesIter = fs.listFiles(sourceWatermarksDir, true);
		    	while (filesIter.hasNext()){
		    		LocatedFileStatus file = filesIter.next();
		    		try (Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file.getPath()))){
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
			    		Utils.writeWatermark(conf, sourceWatermarksDir, partitionId, 0);
			    	}
			    }
		    }
	    }
	    

	    /*
	     * From a hadoop point of view
	     * The input files will be the list of input partitions.
	     * The output files will be the actual data being written
	     * and the dest watermarks will be written out at the end of the
	     * job based on job counters from the mappers.
	     */
	    try(FileSystem fs = destDir.getFileSystem(conf)){
	    	fs.delete(destDir, true);
	    }
	    
	    conf.set("kafka2hadoop.kafkaBroker", kafkaBrokerConnectionStr, "CLI Args");
	    conf.set("kafka2hadoop.kafkaTopic", kafkaTopicName, "CLI Args");
	    
	    Job job = Job.getInstance(getConf(), "kafka2hadoop");
	    job.setJarByClass(Main.class);
	    job.setMapperClass(Kafka2HadoopMapper.class);
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    SequenceFileInputFormat.addInputPath(job, sourceWatermarksDir);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileOutputFormat.setOutputPath(job, destDir);

	    return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);
	}
}
