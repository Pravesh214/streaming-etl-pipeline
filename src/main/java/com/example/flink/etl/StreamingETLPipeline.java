package com.example.flink.etl;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class StreamingETLPipeline {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-consumer");

        DataStream<String> input = env.addSource(
                new FlinkKafkaConsumer<>("raw-input-topic", new SimpleStringSchema(), kafkaProps)
        );

        DataStream<String> transformed = input.map(value -> value.trim().toUpperCase());

        transformed.addSink(
                new FlinkKafkaProducer<>("clean-output-topic", new SimpleStringSchema(), kafkaProps)
        );

        env.execute("Flink Streaming ETL Pipeline");
    }
}