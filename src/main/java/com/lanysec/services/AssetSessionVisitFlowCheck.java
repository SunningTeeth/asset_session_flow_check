package com.lanysec.services;

import com.lanysec.config.JavaKafkaConfigurer;
import com.lanysec.config.ModelParamsConfigurer;
import com.lanysec.utils.ConversionUtil;
import com.lanysec.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author daijb
 * @date 2021/3/27 14:08
 * 资产会话连接流量检测
 * 参数设置参考如下:
 * * --mysql.servers 192.168.3.101
 * * --bootstrap.servers 192.168.3.101:6667
 * * --topic csp_flow //消费
 * * --check.topic csp_event // 建模结果发送的topic
 * * --group.id test
 * * --interval 1m
 */
public class AssetSessionVisitFlowCheck implements AssetSessionVisitConstants {

    private static final Logger logger = LoggerFactory.getLogger(AssetSessionVisitFlowCheck.class);

    public static void main(String[] args) {
        AssetSessionVisitFlowCheck assetSessionVisitFlow = new AssetSessionVisitFlowCheck();
        // 启动任务
        assetSessionVisitFlow.run(args);
    }

    public void run(String[] args) {
        logger.info("[`asset session visit flow check`] flink streaming is starting....");
        StringBuilder text = new StringBuilder(128);
        for (String s : args) {
            text.append(s).append("\t");
        }
        logger.info("all params : " + text.toString());
        Properties properties = JavaKafkaConfigurer.getKafkaProperties(args);
        System.setProperty("mysql.servers", properties.getProperty("mysql.servers"));

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //加载kafka配置信息
        logger.info("load kafka properties : " + properties.toString());
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrap.servers"));
        //可根据实际拉取数据等设置此值，默认30s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        //每次poll的最大数量
        //注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        //当前消费实例所属的消费组
        //属于同一个组的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("group.id"));

        // 启动定时任务
        startFunc();

        // 添加kafka source
        // 过滤kafka无匹配资产的数据
        DataStream<String> kafkaSource = streamExecutionEnvironment.addSource(new FlinkKafkaConsumer010<>(properties.getProperty("topic"), new SimpleStringSchema(), props));

        DataStream<String> kafkaSourceFilter = kafkaSource.filter((FilterFunction<String>) value -> {
            JSONObject line = (JSONObject) JSONValue.parse(value);
            if (!StringUtil.isEmpty(ConversionUtil.toString(line.get("SrcID")))) {
                return true;
            }
            if (StringUtil.isEmpty(ConversionUtil.toString(line.get("DstID")))) {
                return false;
            }
            return false;
        });

        // 添加需要匹配的字段
        DataStream<String> matchAssetSourceStream = kafkaSourceFilter.map(new AssetMapSourceFunction())
                .filter((FilterFunction<String>) value -> !StringUtil.isEmpty(value));

        // 检测系列操作

        SingleOutputStreamOperator<String> checkStreamMap = matchAssetSourceStream.map(new AssetSessionVisitCheckModelSourceFunction());
        SingleOutputStreamOperator<String> matchCheckStreamFilter = checkStreamMap.filter((FilterFunction<String>) value -> !StringUtil.isEmpty(value));

        // 将过滤数据送往kafka  topic
        String brokers = ConversionUtil.toString(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        String checkTopic = ConversionUtil.toString(props.getProperty("check.topic"));
        matchCheckStreamFilter.addSink(new FlinkKafkaProducer010<>(
                brokers,
                checkTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema())
        ));

        try {
            streamExecutionEnvironment.execute("kafka message streaming start ....");
        } catch (Exception e) {
            logger.error("[`asset session visit flow check`] flink streaming execute failed", e);
        }
    }

    private void startFunc() {
        logger.info("starting build model params.....");
        new Timer("timer-model").schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    ModelParamsConfigurer.reloadModelingParams();
                    ModelParamsConfigurer.reloadBuildModelAssetId();
                    ModelParamsConfigurer.reloadBuildModelResult();
                    logger.info("reload model params configurer.");
                } catch (Throwable throwable) {
                    logger.error("timer schedule at fixed rate failed ", throwable);
                }
            }
        }, 1000 * 10, 1000 * 60 * 5);
    }
}
