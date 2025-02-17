package com.larsentoubro.dataextractor.batch;

import com.larsentoubro.dataextractor.jsondata.TableConfig;
import com.larsentoubro.dataextractor.jsondata.TableMapping;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@StepScope
public class UpsertItemWriter implements ItemWriter<Map<String, Object>> {
    private final JdbcTemplate jdbcTemplate;
    private final String targetSchema;
    private final String targetTable;
    private final List<String> primaryKeys;

    @Autowired
    public UpsertItemWriter(@Value("#{jobParameters['targetSchema']}") String targetSchema,
                            @Value("#{jobParameters['targetTable']}") String targetTable,
                            @Value("#{jobParameters['primaryKeys']}") String primaryKeys,
                            @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.primaryKeys = Arrays.asList(primaryKeys.split(","));
    }

    @Override
    public void write(Chunk<? extends Map<String, Object>> items) {
        if (items.isEmpty()) return;

        for (Map<String, Object> item : items) {
            String mergeQuery = buildMergeQuery(item);
            jdbcTemplate.update(mergeQuery, item.values().toArray());
        }
    }

    private String buildMergeQuery(Map<String, Object> item) {
        String fullTableName = targetSchema + "." + targetTable;

        // Columns for insert/update
        List<String> columns = new ArrayList<>(item.keySet());
        List<String> nonPrimaryColumns = columns.stream()
                .filter(col -> !primaryKeys.contains(col))
                .toList();

        // Primary key condition for matching
        String matchCondition = primaryKeys.stream()
                .map(pk -> "target." + pk + " = source." + pk)
                .collect(Collectors.joining(" AND "));

        // Update clause: Only non-primary keys should be updated
        String updateSetClause = nonPrimaryColumns.stream()
                .map(col -> "target." + col + " = source." + col)
                .collect(Collectors.joining(", "));

        // Column list for insert
        String columnList = String.join(", ", columns);
        String valueList = columns.stream()
                .map(col -> "source." + col)
                .collect(Collectors.joining(", "));

        return  "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " ON;" +
                "MERGE INTO " + fullTableName + " AS target " +
                "USING (SELECT ? AS " + String.join(", ? AS ", columns) + ") AS source " +
                "ON " + matchCondition + " " +
                "WHEN MATCHED THEN " +
                "UPDATE SET " + updateSetClause + " " +
                "WHEN NOT MATCHED THEN " +
                "INSERT (" + columnList + ") " +
                "VALUES (" + valueList + ");" +
                "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " OFF;";
    }
}
