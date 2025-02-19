package com.larsentoubro.dataextractor.config;

import com.larsentoubro.dataextractor.batch.DataChangeProcessor;
import com.larsentoubro.dataextractor.batch.SourceTableItemReader;
import com.larsentoubro.dataextractor.batch.UpsertItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;

@Configuration
public class BatchConfig {

    private final DataSource targetDataSource;

    public BatchConfig(@Qualifier("targetDataSource") DataSource targetDataSource) {
        this.targetDataSource = targetDataSource;
    }

    @Bean
    @Scope("prototype")
    public BatchDataSourceScriptDatabaseInitializer batchInitializer(
            @Qualifier("targetDataSource") DataSource batchDataSource,
            BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(
                batchDataSource,
                properties.getJdbc()
        );
    }

    @Bean
    @Scope("prototype")
    public PlatformTransactionManager transactionManager() {
        return new DataSourceTransactionManager(targetDataSource);
    }

    @Bean
    public JobRepository jobRepository(PlatformTransactionManager transactionManager) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDatabaseType("SQLSERVER");
        factory.setDataSource(targetDataSource);
        factory.setTransactionManager(transactionManager);
        factory.setIsolationLevelForCreate("ISOLATION_READ_COMMITTED");
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Bean(name = "upsertJobLauncher")
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        return jobLauncher;
    }

    @Bean
    @StepScope
    public SourceTableItemReader itemReader(@Value("#{jobParameters['sourceSchema']}") String sourceSchema,
                                            @Value("#{jobParameters['sourceTable']}") String sourceTable,
                                            @Qualifier("sourceDataSource") DataSource dataSource) throws IOException {
        return new SourceTableItemReader(sourceSchema, sourceTable, dataSource);
    }

    @Bean
    @StepScope
    public DataChangeProcessor itemProcessor(@Value("#{jobParameters['targetSchema']}") String targetSchema,
                                             @Value("#{jobParameters['targetTable']}") String targetTable,
                                             @Value("#{jobParameters['primaryKeys']}") String primaryKeys,
                                             @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        return new DataChangeProcessor(targetSchema, targetTable, primaryKeys, jdbcTemplate);
    }

    @Bean
    @StepScope
    public UpsertItemWriter itemWriter(@Value("#{jobParameters['targetSchema']}") String targetSchema,
                                       @Value("#{jobParameters['targetTable']}") String targetTable,
                                       @Value("#{jobParameters['primaryKeys']}") String primaryKeys,
                                       @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        return new UpsertItemWriter(targetSchema, targetTable, primaryKeys, jdbcTemplate);
    }

    @Bean
    public Step upsertStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager,
                           ItemReader<Map<String, Object>> itemReader,
                           ItemProcessor<Map<String, Object>, Map<String, Object>> itemProcessor,
                           ItemWriter<Map<String, Object>> itemWriter) {
        return new StepBuilder("upsertStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(100, transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();
    }

    @Bean(name = "upsertJob")
    public Job upsertJob(JobRepository jobRepository, Step upsertStep) {
        return new JobBuilder("upsertJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(upsertStep)
                .build();
    }
}

