package com.bosssoft.job.service;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;

import java.io.IOException;
import java.util.List;


public interface JobService {

    /**
     * 根据接口获取任务明细
     * @return 任务明细
     */
    List<JsonNode> getJobDetails();


    /**
     * 删除hadoop中过时的任务
     * @param jobs 待删除的jobId
     */
    void deleteOutdatedJobs(List<JsonNode> jobs) throws IOException;

}
