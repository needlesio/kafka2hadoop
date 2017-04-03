package io.needles.kafka2hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.verification.VerificationMode;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class Kafka2HadoopMapperTest {

    // Class under test
    private Kafka2HadoopMapper kafka2HadoopMapper;

    @Before
    public void setup(){
        kafka2HadoopMapper = new Kafka2HadoopMapper();
    }

    @Test
    public void testFetchData() throws Exception {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
        String topic = "myTopic";
        Mapper.Context mockContext = mock(Mapper.Context.class);

        addRecords(consumer, topic, 5, 6);
        addRecords(consumer, topic, 6, 8);

        doNothing().when(mockContext).write(text(5), text(5));
        doNothing().when(mockContext).write(text(6), text(6));
        doNothing().when(mockContext).write(text(7), text(7));

        long result = kafka2HadoopMapper.fetchData(consumer, topic, 55, 5, mockContext);

        // Last record was 7...
        assertEquals(8, result);



        addRecords(consumer, topic, 8, 10);

        doNothing().when(mockContext).write(text(8), text(8));
        doNothing().when(mockContext).write(text(9), text(9));

        long result2 = kafka2HadoopMapper.fetchData(consumer, topic, 55, 8, mockContext);

        assertEquals(10, result2);

    }

    private void addRecords(MockConsumer<byte[], byte[]> consumer, String topic, int from, int to) {
        consumer.updateEndOffsets(Collections.singletonMap(new TopicPartition(topic, 55), (long) to));

        consumer.schedulePollTask(()->{
            for (int offset = from; offset < to; offset++) {
                byte[] payload = Integer.toString(offset).getBytes();
                consumer.addRecord(
                        new ConsumerRecord<>(topic, 55, offset, payload, payload)
                );
            }
        });
    }

    private Text text(int v){
        return new Text(Integer.toString(v));
    }


}
