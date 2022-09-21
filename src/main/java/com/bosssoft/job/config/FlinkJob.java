package com.bosssoft.job.config;

import com.bosssoft.job.service.JobService;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

@Component
public class FlinkJob {

    @Resource
    private JobService jobService;

    @Scheduled(cron = "0 0 1 * * ?")
    public void deleteOutdatedJobs() {
        List<JsonNode> jobDetails = jobService.getJobDetails();
        try {
            jobService.deleteOutdatedJobs(jobDetails);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
