package com.bosssoft.job.service.impl;


import com.bosssoft.job.constant.FlinkRestAPIConstant;
import com.bosssoft.job.service.JobService;
import com.bosssoft.job.util.DateUtils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpRequest;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;


import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class JobServiceImpl implements JobService {

    @Value("${flink.historyServer.address}")
    private String historyServerAddr;

    @Value("${flink.checkpoint.dir}")
    private String checkpointDir;

    @Value("{flink.checkpoint.defaultFS}")
    private String defaultFS;

    @Value("{flink.checkpoint.remain}")
    private String  REMAIN_DAYS;

    @Resource
    private RestTemplate restTemplate;
    /**
     * 根据接口获取任务明细
     *
     * @return 返回过时的任务明细
     */
    @Override
    public List<JsonNode> getJobDetails() {
        List<JsonNode> jsonNodes = listJobs();

        //Collection<JobDetails> jobs = multipleJobsDetails.getJobs();
        // 超过3天的加入列表
        List<JsonNode> res = jsonNodes.stream().filter(
                node -> DateUtils.compare(
                        DateUtils.addDays(DateUtils.getCurrentDate(),-Integer.valueOf(REMAIN_DAYS)),
                        DateUtils.timeStampToDate(node.get("end-time").asLong())
                )
        ).collect(Collectors.toList());
        return res;
    }



    public List<JsonNode> listJobs() {
        JsonNode result = restTemplate.getForObject(historyServerAddr + "/jobs/overview", JsonNode.class);
        JsonNode jobs= result.get("jobs");
        List<JsonNode> joblist = new ArrayList<>();
        if (jobs.isArray()) {
            for (final JsonNode objNode : jobs) {
                joblist.add(objNode);
            }
        }
        return joblist;
    }

    /**
     * 删除hadoop中过时的任务
     *
     * @param jobs 待删除的jobId
     */
    @Override
    public void deleteOutdatedJobs(List<JsonNode> jobs)  {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", defaultFS);
        try(FileSystem fileSystem = FileSystem.get(configuration)) {
            jobs.stream().forEach(
                    job -> {
                        try {
                            fileSystem.delete(new Path(checkpointDir + job.get("jid")),true);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
