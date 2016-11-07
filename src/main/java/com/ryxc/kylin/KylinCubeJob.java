package com.ryxc.kylin;

import com.alibaba.fastjson.JSON;
import com.ryxc.kylin.exception.CubeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tangfei on 2016/11/7.
 */
public class KylinCubeJob extends Configured implements Tool {
    private static Logger logger = LoggerFactory.getLogger(KylinCubeJob.class);
    private static final String BASE_URL = "http://10.9.12.11:7070/kylin/api";
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");



    public String rebuildCube(String cubeName, long startTime, long endTime, String buildType)
            throws CubeException {
        String path = "/cubes/" + cubeName + "/rebuild";

        Map dataMap = new HashMap();
        dataMap.put("startTime", Long.valueOf(startTime));
        dataMap.put("endTime", Long.valueOf(endTime));
        dataMap.put("buildType", buildType);

        String jsonData = JSON.toJSONString(dataMap);

        String result = null;
        try {
            result = Request.Put("http://10.9.12.11:7070/kylin/api" + path)
                    .addHeader("accept", "application/json")
                    .addHeader("Content-Type", "application/json")
                    .addHeader("Authorization", "Basic QURNSU46S1lMSU4=")
                    .bodyString(jsonData, ContentType.APPLICATION_JSON)
                    .connectTimeout(180000)
                    .execute().returnContent().asString();
        } catch (IOException e) {
            throw new CubeException("Request " + path + "\n\tParams:\n\t" + jsonData + "\n\tErrorInfo: " + e.getMessage());
        }

        if ((result == null) || ("".equals(result))) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            dataMap.put("startTime", sdf.format(new Date(startTime)));
            dataMap.put("endTime", sdf.format(new Date(endTime)));
            throw new CubeException("Request " + path + "\n\tError:\n 参数有误\n\tParams:\n\t" + JSON.toJSONString(dataMap));
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new RuntimeException("参数异常" + args);
        }
        int res = ToolRunner.run(new Configuration(), new KylinCubeJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String cubeName = args[0];
        String startTimeStr = args[1];
        String buildType = args[2];

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long startTime = sdf.parse(startTimeStr).getTime();
        long endTime = startTime + 86400000L;

        KylinCubeJob cube = new KylinCubeJob();
        String result = cube.rebuildCube(cubeName, startTime, endTime, buildType);
        StringBuilder msgSb = new StringBuilder("Build Info:\n");
        Map resultMapper = (Map)JSON.parseObject(result, Map.class);
        msgSb.append("\tuuid: ").append(resultMapper.get("uuid")).append("\n");
        msgSb.append("\tname: ").append(resultMapper.get("name")).append("\n");
        msgSb.append("\ttype: ").append(resultMapper.get("type")).append("\n");
        msgSb.append("\tjob_status: ").append(resultMapper.get("job_status")).append("\n");
        msgSb.append("\tsubmitter: ").append(resultMapper.get("submitter")).append("\n");
        logger.info("cube-result:"+msgSb.toString());
        return 0;
    }

}
