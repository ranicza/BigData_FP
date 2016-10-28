package com.epam.bigdata.sparkstreaming.kafka;


import com.epam.bigdata.conf.AppProperties;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class KafkaProcessor {
    public static JavaPairReceiverInputDStream<String,String> getStream(JavaStreamingContext jsc, AppProperties.KafkaConnection kafkaConf){
        return KafkaUtils.createStream(
                jsc,
                kafkaConf.getZookeeper(),
                kafkaConf.getGroup(),
                kafkaConf.getTopics()
        );
    }
}
