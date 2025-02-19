package com.larsentoubro.dataextractor.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
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
    @Scope("prototype")
    public DataSource sourceDataSource(@Value("${spring.datasource.source.database:database=IOT_STORE;}") String sourceDatabase) {
        return DataSourceBuilder.create()
                .url(sourceUrl + sourceDatabase)
                .username(sourceUsername)
                .password(sourcePassword)
                .build();
    }

    @Primary
    @Bean(name = "targetDataSource")
    @Scope("prototype") // Ensures a new instance is created dynamically
    public DataSource targetDataSource(@Value("${spring.datasource.target.database:database=IOT_STORE_bronze;}") String targetDatabase) {
        String fullUrl = targetUrl + targetDatabase;

        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl(fullUrl);
        hikariDataSource.setUsername(targetUsername);
        hikariDataSource.setPassword(targetPassword);
        hikariDataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        return hikariDataSource;
    }


    @Bean(name = "sourceJdbcTemplate")
    @Scope("prototype")
    public JdbcTemplate sourceJdbcTemplate(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "targetJdbcTemplate")
    @Scope("prototype")
    public JdbcTemplate targetJdbcTemplate(@Qualifier("targetDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}

