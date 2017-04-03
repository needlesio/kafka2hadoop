package io.needles.kafka2hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static io.needles.kafka2hadoop.Kafka2HadoopMapper.*;

public class MainTest {

    // Class under test
    private Main main;

    @Before
    public void setup(){
        main = new Main();
        main.setConf(new Configuration(false));
    }

    @Test
    public void testZeroArgs() throws Exception {
        int exitCode = main.run();
        assertNotEquals(0, exitCode);
    }

    @Test
    public void testBuildJob() throws Exception {
        Path sourceWatermarks = new Path("sourceWatermarks");
        Path destWatermarks = new Path("destWatermarks");
        Job job = main.buildJob("connStr", "topicName", sourceWatermarks, destWatermarks);
        Configuration configuration = job.getConfiguration();

        assertEquals(0, job.getNumReduceTasks());
        assertEquals("connStr", configuration.get(KAFKA_BROKER_CONNECTION_STRING));
        assertEquals("topicName", configuration.get(KAFKA_TOPIC));
        assertEquals(Kafka2HadoopMapper.class, job.getMapperClass());

        FileSystem fs = sourceWatermarks.getFileSystem(configuration);
        // MR Input
        assertArrayEquals(new Path[] {fs.makeQualified(sourceWatermarks)}, FileInputFormat.getInputPaths(job));

        // MR Output
        assertEquals(fs.makeQualified(destWatermarks), FileOutputFormat.getOutputPath(job));
        assertEquals(Text.class, job.getMapOutputKeyClass());
        assertEquals(Text.class, job.getMapOutputValueClass());
    }

}
