package com.larsentoubro.dataextractor.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@StepScope
public class SourceTableItemReader extends JdbcPagingItemReader<Map<String, Object>> {

    @Autowired
    public SourceTableItemReader(@Value("#{jobParameters['sourceSchema']}") String sourceSchema,
                                 @Value("#{jobParameters['sourceTable']}") String sourceTable,
                                 @Value("#{jobParameters['primaryKeys']}") String primaryKeysCsv,
                                 @Qualifier("sourceDataSource") DataSource dataSource) {

        setDataSource(dataSource);
        setPageSize(5000);  // Optimized for large tables
        setFetchSize(5000);
        setRowMapper(new ColumnMapRowMapper());

        List<String> primaryKeys = Arrays.asList(primaryKeysCsv.split(","));

        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("*");
        queryProvider.setFromClause(sourceSchema + "." + sourceTable);
        queryProvider.setSortKeys(primaryKeys.stream().collect(Collectors.toMap(pk -> pk, pk -> Order.ASCENDING)));

        try {
            setQueryProvider(queryProvider.getObject());
        } catch (Exception e) {
            throw new RuntimeException("Failed to set query provider", e);
        }

        log.info("Configured reader for table: {}.{} with primary keys: {}", sourceSchema, sourceTable, primaryKeys);
    }
}
