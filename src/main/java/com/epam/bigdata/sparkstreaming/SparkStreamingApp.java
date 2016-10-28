package com.epam.bigdata.sparkstreaming;

import com.epam.bigdata.sparkstreaming.model.CityInfoEntity;
import com.epam.bigdata.sparkstreaming.model.LogsEntity;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.bitwalker.useragentutils.UserAgent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.net.URI;

public class SparkStreamingApp {

    private static final String SPLIT = "\\t";
    private static ObjectMapper mapper = new ObjectMapper();
    private static final String formatter = "yyyy-MM-dd'T'HH:mm:ssX";

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.err.println("Usage: SparkStreamingLogAggregationApp {zkQuorum} {group} {topic} {numThreads} {cityPaths}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.parseInt(args[3]);

        List<String> allCities = FileHelper.getLinesFromFile(args[4]);
        HashMap<Integer, CityInfoEntity> cityInfoMap = new HashMap<>();
        allCities.forEach(city -> {
            String[] fields = city.split(SPLIT);
            cityInfoMap.put(Integer.parseInt(fields[0]), /*geoPoint*/new CityInfoEntity(Float.parseFloat(fields[6]), Float.parseFloat(fields[7])));          
        });

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("es.index.auto.create", "true");



        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Broadcast<Map<Integer, CityInfoEntity>> broadcastVar = jssc.sparkContext().broadcast(cityInfoMap);

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(tuple2 -> {         
            UserAgent ua = UserAgent.parseUserAgentString(tuple2._2().toString());
			String device =  ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
			String osName = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;
            String uaFamily = ua.getBrowser() != null ? ua.getBrowser().getGroup().getName() : null;
            
            LogsEntity logsEntity = new LogsEntity(tuple2._2().toString());
            logsEntity.setGeoPoint1(broadcastVar.value().get(logsEntity.getCity()));
            logsEntity.setDevice(device);
            logsEntity.setOsName(osName);
            logsEntity.setUaFamily(uaFamily);
            
            JSONObject jsonObject = new JSONObject(logsEntity);
            jsonObject.append("@sended_at",new java.text.SimpleDateFormat(formatter).format(new Date()));
            String json1 = jsonObject.toString();
            //String json1  = mapper.writeValueAsString(logsEntity);
            System.out.println("####1");

            System.out.println(json1);
            return json1;
        });

        lines.foreachRDD(stringJavaRDD ->
                JavaEsSpark.saveJsonToEs(stringJavaRDD, "logs02/logs"));
        
        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }
//    
//    private static String replaceSpace(String str) {
//    	return str.replaceAll("\\s","_");
//    }
}