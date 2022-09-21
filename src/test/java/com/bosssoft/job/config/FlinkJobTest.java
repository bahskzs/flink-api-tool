package com.bosssoft.job.config;

import com.bosssoft.job.service.JobService;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(SpringRunner.class)
@SpringBootTest
class FlinkJobTest {

    @Resource
    private  JobService jobService;

    @Test
    void deleteOutdatedJobs() {
        List<JsonNode> jobDetails = jobService.getJobDetails();
        try {
            jobService.deleteOutdatedJobs(jobDetails);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}