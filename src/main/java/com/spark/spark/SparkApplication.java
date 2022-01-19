package com.spark.spark;

//import org.apache.spark.streaming.Duration;
//import java.util.*;
//import org.apache.spark.SparkConf;
//import org.apache.spark.TaskContext;
//import org.apache.spark.api.java.*;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.streaming.api.java.*;
//
//import kafka.serializer.StringDecoder;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Tuple2;


public class SparkApplication {

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        org.apache.log4j.Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("viewingFigures");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(500));

        Collection<String> topics = Arrays.asList("topic1");

        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", "localhost:9092");
        params.put("key.deserializer", StringDeserializer.class);
        params.put("value.deserializer", StringDeserializer.class);
        params.put("group.id", "spark-group");
        params.put("auto.offset.reset", "latest");
        params.put("enable.auto.commit", true);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, params));



        JavaDStream<String> data = stream.map(v -> {
            return v.value();    // mapping to convert into spark D-Stream
        });

        data.print();

//        JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<>(item.value(),5L))
//                .reduceByKeyAndWindow((x, y) -> x + y, Durations.minutes(60), Durations.seconds(5) )
//                .mapToPair(item -> item.swap())
//                .transformToPair(rdd -> rdd.sortByKey(false));
//
//        results.print(50);

        sc.start();
        sc.awaitTermination();
    }

}
