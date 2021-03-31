package com.lanysec.config;

import com.lanysec.services.AssetSessionVisitConstants;
import com.lanysec.utils.ConversionUtil;
import com.lanysec.utils.DbConnectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author daijb
 * @date 2021/3/27 14:59
 */
public class ModelParamsConfigurer implements AssetSessionVisitConstants {

    private static final Logger logger = LoggerFactory.getLogger(ModelParamsConfigurer.class);

    private static volatile Map<String, Object> modelingParams;

    /**
     * 返回建模参数
     *
     * @return 建模参数k-v
     */
    public static Map<String, Object> getModelingParams() {
        if (modelingParams == null) {
            modelingParams = reloadModelingParams();
        }
        return modelingParams;
    }

    /**
     * 从数据库获取建模参数
     */
    public static Map<String, Object> reloadModelingParams() {
        Map<String, Object> result = new HashMap<>(20);
        try {
            Connection conn = DbConnectUtil.getConnection();
            if (conn == null) {
                return result;
            }
            String sql = "select * from modeling_params" +
                    " where model_type=1 and model_child_type =1" +
                    " and model_switch = 1 and model_switch_2 =1";
            //+" and modify_time < DATE_SUB( NOW(), INTERVAL 10 MINUTE );";
            ResultSet resultSet = conn.createStatement().executeQuery(sql);
            while (resultSet.next()) {
                result.put(MODEL_ID, resultSet.getString("id"));
                result.put(MODEL_TYPE, resultSet.getString("model_type"));
                result.put(MODEL_CHILD_TYPE, resultSet.getString("model_child_type"));
                result.put(MODEL_RATE_TIME_UNIT, resultSet.getString("model_rate_timeunit"));
                result.put(MODEL_RATE_TIME_UNIT_NUM, resultSet.getString("model_rate_timeunit_num"));
                result.put(MODEL_RESULT_SPAN, resultSet.getString("model_result_span"));
                result.put(MODEL_RESULT_TEMPLATE, resultSet.getString("model_result_template"));
                result.put(MODEL_CONFIDENCE_INTERVAL, resultSet.getString("model_confidence_interval"));
                result.put(MODEL_HISTORY_DATA_SPAN, resultSet.getString("model_history_data_span"));
                result.put(MODEL_UPDATE, resultSet.getString("model_update"));
                result.put(MODEL_SWITCH, resultSet.getString("model_switch"));
                result.put(MODEL_SWITCH_2, resultSet.getString("model_switch_2"));
                result.put(MODEL_ATTRS, resultSet.getString("model_alt_params"));
                result.put(MODEL_TASK_STATUS, resultSet.getString("model_task_status"));
                result.put(MODEL_MODIFY_TIME, resultSet.getString("modify_time"));
            }
        } catch (Throwable throwable) {
            logger.error("Get modeling parameters from the database error ", throwable);
        }
        logger.info("Get modeling parameters from the database : " + result.toString());
        modelingParams = result;
        return result;
    }

    private static volatile Set<String> allAssetIds;

    /**
     * 返回建模参数
     *
     * @return 建模参数k-v
     */
    public static Set<String> getAllAssetIds() throws Exception {
        if (allAssetIds == null) {
            allAssetIds = reloadBuildModelAssetId();
        }
        return allAssetIds;
    }

    /**
     * 重新加载建模资产id
     */
    public static Set<String> reloadBuildModelAssetId() throws SQLException {
        String sql = "SELECT entity_id FROM group_members g,modeling_params m " +
                "WHERE m.model_alt_params -> '$.model_entity_group' LIKE CONCAT('%', g.group_id,'%') " +
                "and m.model_type=1 and model_child_type=1 " +
                "and m.model_switch=1 and m.model_switch_2=1";
        Set<String> result = new HashSet<>();
        Connection conn = DbConnectUtil.getConnection();
        if (conn != null) {
            ResultSet resultSet = conn.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                String entityId = ConversionUtil.toString(resultSet.getString("entity_id"));
                result.add(entityId);
            }
        }
        allAssetIds = result;
        return result;
    }

    /**
     * 存储当前模型建模结果
     * key : modelingParamsId
     * value : k: srcId ===> v : 一条记录
     */
    private static volatile List<Map<String, Object>> modelResults;

    /**
     * 返回建模参数
     *
     * @return 建模参数k-v
     */
    public static List<Map<String, Object>> getModelResults() throws Exception {
        if (modelResults == null) {
            modelResults = reloadBuildModelResult();
        }
        return modelResults;
    }

    /**
     * 查找建模结果
     */
    public static List<Map<String, Object>> reloadBuildModelResult() throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();
        String sql = "SELECT " +
                "c.modeling_params_id,r.flow,r.src_ip,r.src_id,r.protocol,r.up_down,c.model_check_alt_params " +
                "FROM model_result_asset_session_flow r,model_check_params c " +
                "WHERE " +
                "r.modeling_params_id = c.modeling_params_id " +
                "AND c.model_type = 1 " +
                "AND c.model_child_type = 1 " +
                "AND c.model_check_switch = 1 " +
                "AND c.modify_time > DATE_SUB( NOW(), INTERVAL 10 MINUTE );";
        Connection conn = DbConnectUtil.getConnection();
        if (conn == null) {
            return result;
        }
        ResultSet resultSet = conn.prepareStatement(sql).executeQuery();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<>(5);
            String modelingParamsId = ConversionUtil.toString(resultSet.getString("modeling_params_id"));
            String flowSegment = ConversionUtil.toString(resultSet.getString("flow"));
            String srcIp = ConversionUtil.toString(resultSet.getString("src_ip"));
            String srcId = ConversionUtil.toString(resultSet.getString("src_id"));
            String protocol = ConversionUtil.toString(resultSet.getString("protocol"));
            String upDown = ConversionUtil.toString(resultSet.getString("up_down"));
            String modelCheckAltParams = ConversionUtil.toString(resultSet.getString("model_check_alt_params"));
            map.put("modelingParamsId", modelingParamsId);
            map.put("flowSegment", flowSegment);
            map.put("srcIp", srcIp);
            map.put("srcId", srcId);
            map.put("protocol", protocol);
            map.put("upDown", upDown);
            map.put("modelCheckAltParams", modelCheckAltParams);
            result.add(map);
        }
        modelResults = result;
        return result;
    }
}
