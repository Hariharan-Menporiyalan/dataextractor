package com.larsentoubro.dataextractor.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {
    @Value("${spring.datasource.source.url}")
    public String sourceUrl;

    @Value("${spring.datasource.source.username}")
    public String sourceUsername;

    @Value("${spring.datasource.source.password}")
    public String sourcePassword;

    @Value("${spring.datasource.target.url}")
    public String targetUrl;

    @Value("${spring.datasource.target.username}")
    public String targetUsername;

    @Value("${spring.datasource.target.password}")
    public String targetPassword;

    @Bean(name = "sourceDataSource")
    public DataSource sourceDataSource() {
        return DataSourceBuilder.create()
                .url(sourceUrl)
                .username(sourceUsername)
                .password(sourcePassword)
                .build();
    }

    @Primary
    @Bean(name = "targetDataSource")
    public DataSource targetDataSource() {
        return DataSourceBuilder.create()
                .url(targetUrl)
                .username(targetUsername)
                .password(targetPassword)
                .build();
    }

    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchInitializer(
            @Qualifier("targetDataSource") DataSource batchDataSource,
            BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(
                batchDataSource,
                properties.getJdbc()
        );
    }

    @Bean(name = "sourceJdbcTemplate")
    public JdbcTemplate sourceJdbcTemplate(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "targetJdbcTemplate")
    public JdbcTemplate targetJdbcTemplate(@Qualifier("targetDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}

