package com.lanysec.services;

import com.lanysec.config.ModelParamsConfigurer;
import com.lanysec.utils.ConversionUtil;
import com.lanysec.utils.DbConnectUtil;
import com.lanysec.utils.StringUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @author daijb
 * @date 2021/3/27 14:13
 * 资产会话流量检测
 * <p>
 * * 从mysql表 model_check_params 获取检测参数：select * from model_check_params where model_type=1 and model_child_type =1 and model_check_switch= 1 and now()-modify_time > 10min;
 * *
 * * 排除白名单：根据字段 model_check_alt_params：white_ip_list
 * *
 * * 检测逻辑：
 * *
 * * 1.csp_flow 源数据字段：SrcIP、SrcPort、SrcID、DstIP、DstPort、@timestamp、ID、L4P、L7P；
 * *
 * * 2.根据建模结果对应的时间段，检测对应资产连接的不同协议的流量大小是否在建模结果范围内。
 * *
 * * 例，某资产在每天的8时TCP访问流量是300-400。如果在同样的时间访问TCP超过400则上报异常
 * *
 * * 3.构造结果数据发送到目的Topic
 */
public class AssetSessionVisitCheckModelSourceFunction extends RichMapFunction<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(AssetSessionVisitCheckModelSourceFunction.class);

    @Override
    public String map(String line) throws Exception {
        //{"L7P":"tls","InPackets":20,"ClusterID":"EWN7S4UJ","L3P":"IP","OutFlow":1350,"OutPackets":10,"OutFlags":0,"InFlow":1950,"rHost":"192.168.9.58","SrcID":"ast_458b6b75b0610d081dc83c7c6a34498a","ID":"fle_TTJoPs5YQoTQwZqGbkfvmG","sTime":1615090860000,"SrcCountry":"中国北京","rType":"1","SrcPort":17339,"DstLocName":"杭州","eTime":1615090981000,"AreaID":19778692,"L4P":"TCP","DstCountry":"中国","InFlags":0,"MetaID":"evm_flow","DstPort":443,"SrcIP":"192.168.7.249","ESMetaID":"esm_flow","SID":"1296f58e4f3ab1c3ced4bce072532608","FlowID":"1296f58e4f3ab1c3ced4bce072532608","rTime":1615090981139,"@timestamp":1615090995592,"DstID":"","DstIP":"47.111.111.35","DstMAC":"bc:3f:8f:63:6c:80","PcapID":"","TrafficSource":"eth1","SrcMAC":"4c:cc:6a:57:95:8a","Key":"","SrcLocName":"未知"}
        JSONObject json = (JSONObject) JSONValue.parse(line);
        String srcId = ConversionUtil.toString(json.get("SrcID"));
        String srcIp = ConversionUtil.toString(json.get("SrcIP"));
        String dstIp = ConversionUtil.toString(json.get("DstIP"));
        String l4P = ConversionUtil.toString(json.get("L4P"));
        int inFlow = ConversionUtil.toInt(json.get("InFlow"));
        int outFlow = ConversionUtil.toInt(json.get("OutFlow"));

        String key = ConversionUtil.toString(calculateSegmentCurrKey());
        List<Map<String, Object>> modelResults = ModelParamsConfigurer.getModelResults();
        for (Map<String, Object> map : modelResults) {

            //TODO 排除白名单 放行
            JSONObject obj = (JSONObject) JSONValue.parse(ConversionUtil.toString(map.get("modelCheckAltParams")));
            Object whiteIps = obj.get("white_ip_list");
            if (whiteIps != null) {
                JSONArray whiteIpsList = (JSONArray) JSONValue.parse(whiteIps.toString());
                if (whiteIpsList != null && !whiteIpsList.isEmpty()) {
                    Set whileIpSet = new HashSet(whiteIpsList);
                    if (whileIpSet.contains(srcIp)) {
                        return null;
                    }
                    if (whileIpSet.contains(dstIp)) {
                        return null;
                    }
                }
            }

            if (StringUtil.isEmpty(l4P) || StringUtil.isEmpty(srcIp)) {
                return line;
            }

            String protocol = ConversionUtil.toString(map.get("protocol"));
            // 上下行区分。0 下行流量 in ;1 上行流量 out'
            String upDown = ConversionUtil.toString(map.get("upDown"));
            String entityId = ConversionUtil.toString(map.get("srcId"));
            if (StringUtil.equals(srcId, entityId) && StringUtil.equals(protocol, l4P)) {

                String flowSegment = ConversionUtil.toString(map.get("flowSegment"));
                if (StringUtil.isEmpty(flowSegment)) {
                    return line;
                }
                JSONArray flowSegmentArr = (JSONArray) JSONValue.parse(flowSegment);
                if (flowSegmentArr == null || flowSegmentArr.isEmpty()) {
                    return line;
                }
                String flowSizeSection = null;
                for (Object o : flowSegmentArr) {
                    JSONObject item = (JSONObject) JSONValue.parse(ConversionUtil.toString(o));
                    String name = ConversionUtil.toString(item.get("name"));
                    if (StringUtil.equals(name, key)) {
                        flowSizeSection = ConversionUtil.toString(item.get("value"));
                    }
                }
                if (StringUtil.isEmpty(flowSizeSection)) {
                    return line;
                }
                int cursor = flowSizeSection.indexOf("-");
                int startFlowSize = ConversionUtil.toInt(flowSizeSection.substring(0, cursor));
                int endFlowSize = ConversionUtil.toInt(flowSizeSection.substring(cursor + 1));
                if (ConversionUtil.toBoolean(upDown)) {
                    // 上行 inFlow
                    if (outFlow > endFlowSize) {
                        return line;
                    }
                } else {
                    //下行 outFlow
                    if (inFlow > endFlowSize) {
                        return line;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 建模结果周期：1 代表一天。
     * 2 代表一周。3 代表一季度。
     * 4 代表一年。如果总长度填写 1 ，建模的时间单位可以是 ss mm hh 。周的建模时间单位只能是 dd 。其他的只能为月
     * 计算模型的key
     * <pre>
     *   1. 建模周期为天 ：
     *       SegmentKey : 每次从当前日期开始计算,以小时为key
     *   2. 建模周期为周：建模时间单位只能是天）
     *       SegmentKey : 每次从当前日期开始计算,以当前周几为key
     *   3. 建模周期为季度：（建模时间单位只能是月）
     *      SegmentKey : 每次从当前日期开始计算,以当前月份开始递增
     *   4. 建模周期为年：（建模时间单位只能是月）
     *      SegmentKey : 每次从当前日期开始计算,以当前月份开始递增
     *
     * </pre>
     */
    private Object calculateSegmentCurrKey() throws Exception {
        // 建模周期
        int cycle = ConversionUtil.toInteger(ModelParamsConfigurer.getModelingParams().get(AssetSessionVisitConstants.MODEL_RESULT_SPAN));
        LocalDateTime now = LocalDateTime.now();
        Object segmentKey = null;
        switch (cycle) {
            // 暂时默认为小时
            case 1: {
                segmentKey = now.getHour();
                break;
            }
            //周,频率只能是 dd
            case 2: {
                segmentKey = now.getDayOfWeek().getValue();
                break;
            }
            //季度,频率只能是月
            case 3:
            case 4: {
                // 年,频率只能是月
                segmentKey = now.getMonth().getValue();
                break;
            }
            default: {
                throw new Exception("modeling span is not support.");
            }
        }
        return segmentKey;
    }
}
