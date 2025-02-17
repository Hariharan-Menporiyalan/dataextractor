package com.larsentoubro.dataextractor.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;

@Slf4j
@Component
@StepScope
public class SourceTableItemReader extends JdbcCursorItemReader<Map<String, Object>> {

    @Autowired
    public SourceTableItemReader(@Value("#{jobParameters['sourceSchema']}") String sourceSchema,
                                 @Value("#{jobParameters['sourceTable']}") String sourceTable,
                                 @Qualifier("sourceDataSource") DataSource dataSource) {
        setDataSource(dataSource);
        setRowMapper(new ColumnMapRowMapper());
        setSql("SELECT * FROM " + sourceSchema + "." + sourceTable);

        log.info("Executing query: SELECT * FROM {}.{}", sourceSchema, sourceTable);
    }
}








