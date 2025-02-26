package com.larsentoubro.dataextractor.service;

import com.larsentoubro.dataextractor.jsondata.TableConfig;
import com.larsentoubro.dataextractor.jsondata.TableMapping;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
public class DataExtractorService {

    private final ApplicationContext applicationContext;
    private final JobLauncher jobLauncher;
    private final Job upsertJob;
    private final TableConfigLoader tableConfigLoader;

    @Value("${spring.datasource.target.url}")
    private String targetUrl;

    public DataExtractorService(ApplicationContext applicationContext, @Qualifier("upsertJobLauncher") JobLauncher jobLauncher,
                                @Qualifier("upsertJob") Job upsertJob, TableConfigLoader tableConfigLoader) {
        this.applicationContext = applicationContext;
        this.jobLauncher = jobLauncher;
        this.upsertJob = upsertJob;
        this.tableConfigLoader = tableConfigLoader;
    }

    private void initializeBatchMetadata(String targetDatabase) {
        try {
            DataSource targetDataSource = applicationContext.getBean("targetDataSource", DataSource.class);
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.addScript(new ClassPathResource("create-batch-tables.sql"));
            populator.setSeparator("GO");

            try (Connection connection = targetDataSource.getConnection()) {
                populator.populate(connection);
                log.info("Batch tables initialized in database: {}", targetDatabase);
            }
            log.info("Initializing batch tables in database: {}", targetDataSource);

        } catch (Exception e) {
            log.error("Failed to initialize batch tables in database: {}", targetDatabase, e);
        }
    }

    public void runBatchJob() {
        try {
            List<TableConfig> tableConfigs = tableConfigLoader.loadTableConfig();

            for (TableConfig tableConfig : tableConfigs) {
                String sourceDatabaseName = tableConfig.getSourceDatabase();
                String targetDatabaseName = sourceDatabaseName + "_bronze";

                //this is for test
                targetDatabaseName = sourceDatabaseName;

                for (TableMapping tableMapping : tableConfig.getTablesForChanges()) {
                    String targetSchema = tableMapping.getTargetSchema();
                    String primaryKeys = String.join(",", tableMapping.getPrimaryKey());

                    // Ensure batch metadata tables exist before job starts
                    initializeBatchMetadata(targetDatabaseName);

                    System.setProperty("spring.datasource.source.database", "database=" + sourceDatabaseName + ";" + "defaultSchema=dbo;");
                    System.setProperty("spring.datasource.target.database", "database=" + targetDatabaseName + ";" + "defaultSchema=dbo;");

                    log.info("Batch metadata initialized for targetDatabase: {}, targetSchema: {}", targetDatabaseName, targetSchema);

                    // Manually update targetDataSource dynamically
                    DataSource targetDataSource = applicationContext.getBean("targetDataSource", DataSource.class);
                    log.info("New Target DataSource created: {}", targetDataSource);

                    log.info("Target DataSource updated to: {}", targetUrl + "database=" + targetDatabaseName + ";");

                    JobParameters jobParameters = new JobParametersBuilder()
                            .addString("sourceDatabase", sourceDatabaseName)
                            .addString("sourceSchema", tableMapping.getSourceSchema())
                            .addString("sourceTable", tableMapping.getSourceTable())
                            .addString("primaryKeys", primaryKeys)
                            .addString("targetDatabase", targetDatabaseName)
                            .addString("targetSchema", targetSchema)
                            .addString("targetTable", tableMapping.getTargetTable())
                            .addLong("time", System.currentTimeMillis())
                            .toJobParameters();

                    log.info("Starting batch job for targetDatabase: {}, targetSchema: {}, targetTable: {}", targetDatabaseName, targetSchema, tableMapping.getTargetTable());

                    JobExecution execution = jobLauncher.run(upsertJob, jobParameters);
                    log.info("Job Status for targetDatabase: {}, targetSchema: {}, targetTable: {} -> {}", targetDatabaseName, targetSchema, tableMapping.getTargetTable(), execution.getStatus());
                }
            }
        } catch (Exception e) {
            log.error("Batch job execution failed", e);
        }
    }

//    @Scheduled(cron = "0 */2 * * * *")
    @EventListener(ApplicationReadyEvent.class)
    public void runJobOnSchedule() {
        log.info("Running scheduled batch job at {}...", LocalDateTime.now().toLocalTime());
        runBatchJob();
    }
}

