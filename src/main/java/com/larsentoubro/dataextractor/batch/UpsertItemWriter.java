package com.larsentoubro.dataextractor.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
                            @Value("#{jobParameters['primaryKeys']}") String primaryKeysCsv,
                            @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.primaryKeys = Arrays.asList(primaryKeysCsv.split(","));
    }

    @Override
    public void write(Chunk<? extends Map<String, Object>> items) {
        if (items.isEmpty()) return;

        List<Map<String, Object>> batch = new ArrayList<>(items.getItems());
        String sql = buildMergeQuery(batch);

        jdbcTemplate.batchUpdate(sql, batch.stream()
                .map(item -> item.values().toArray())
                .toList());
    }

    private String buildMergeQuery(List<Map<String, Object>> batch) {
        String fullTableName = targetSchema + "." + targetTable;
        List<String> columns = new ArrayList<>(batch.get(0).keySet());
        List<String> nonPrimaryColumns = columns.stream().filter(col -> !primaryKeys.contains(col)).toList();

        String matchCondition = primaryKeys.stream().map(pk -> "t." + pk + " = s." + pk).collect(Collectors.joining(" AND "));
        String updateSetClause = nonPrimaryColumns.stream().map(col -> "t." + col + " = s." + col).collect(Collectors.joining(", "));

        return  "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " ON;" +
                "MERGE INTO " + fullTableName + " AS t " +
                "USING (SELECT ? AS " + String.join(", ? AS ", columns) + ") AS s " +
                "ON " + matchCondition + " " +
                "WHEN MATCHED THEN UPDATE SET " + updateSetClause + " " +
                "WHEN NOT MATCHED THEN INSERT (" + String.join(", ", columns) + ") VALUES (" +
                columns.stream().map(c -> "s." + c).collect(Collectors.joining(", ")) + ");" +
                "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " OFF;";
    }
}