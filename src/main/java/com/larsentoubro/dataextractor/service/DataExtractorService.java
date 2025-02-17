package com.larsentoubro.dataextractor.service;

import com.larsentoubro.dataextractor.jsondata.TableConfig;
import com.larsentoubro.dataextractor.jsondata.TableMapping;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class DataExtractorService {
    private final JobLauncher jobLauncher;
    private final Job upsertJob;
    private final TableConfigLoader tableConfigLoader;

    @Autowired
    public DataExtractorService(JobLauncher jobLauncher, Job upsertJob, TableConfigLoader tableConfigLoader) {
        this.jobLauncher = jobLauncher;
        this.upsertJob = upsertJob;
        this.tableConfigLoader = tableConfigLoader;
    }

    public void runBatchJob() {
        try {
            List<TableConfig> tableConfigs = tableConfigLoader.loadTableConfig();

            for (TableConfig tableConfig : tableConfigs) {
                for (TableMapping tableMapping : tableConfig.getTablesForChanges()) {

                    String primaryKeys = String.join(",", tableMapping.getPrimaryKey());

                    JobParameters jobParameters = new JobParametersBuilder()
                            .addString("sourceSchema", tableMapping.getSourceSchema())
                            .addString("sourceTable", tableMapping.getSourceTable())
                            .addString("targetSchema", tableMapping.getTargetSchema())
                            .addString("targetTable", tableMapping.getTargetTable())
                            .addString("primaryKeys", primaryKeys)
                            .addLong("time", System.currentTimeMillis())
                            .toJobParameters();

                    log.info("Starting batch job for table: {}.{}", tableMapping.getSourceSchema(), tableMapping.getSourceTable());

                    JobExecution execution = jobLauncher.run(upsertJob, jobParameters);
                    log.info("Job Status for {}.{} -> {}", tableMapping.getSourceSchema(), tableMapping.getSourceTable(), execution.getStatus());
                }
            }
        } catch (Exception e) {
            log.error("Batch job execution failed", e);
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void runJobOnSchedule() {
        log.info("Running scheduled batch job...");
        runBatchJob();
    }
}

