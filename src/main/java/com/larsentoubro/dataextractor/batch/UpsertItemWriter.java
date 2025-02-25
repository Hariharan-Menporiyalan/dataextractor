package com.larsentoubro.dataextractor.batch;

import jakarta.transaction.Transactional;
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
                            @Value("#{jobParameters['primaryKeys']}") String primaryKeysCsv,
                            @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.primaryKeys = Arrays.asList(primaryKeysCsv.split(","));
    }

    @Override
    @Transactional
    public void write(Chunk<? extends Map<String, Object>> items) {
        if (items.isEmpty()) return;

        List<Map<String, Object>> batch = new ArrayList<>(items.getItems());
        String sql = buildMergeQuery(batch);

        List<Object[]> batchParams = new ArrayList<>();
        for (Map<String, Object> item : batch) {
            Object[] params = item.values().toArray();
            batchParams.add(params);
        }

        jdbcTemplate.batchUpdate(sql, batchParams);
    }

    private String buildMergeQuery(List<Map<String, Object>> batch) {
        String fullTableName = targetSchema + "." + targetTable;
        List<String> columns = new ArrayList<>(batch.get(0).keySet());

        return "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " ON; " +
                "MERGE INTO " + targetSchema + "." + targetTable + " AS t " +
                "USING (SELECT ? AS " + String.join(", ? AS ", columns) + ") AS s " +
                "ON " + primaryKeys.stream().map(pk -> "t.[" + pk + "] = s.[" + pk + "]").collect(Collectors.joining(" AND ")) + " " +
                "WHEN MATCHED THEN " +
                "UPDATE SET " +
                columns.stream().filter(col -> !primaryKeys.contains(col)).map(col -> "t.[" + col + "] = s.[" + col + "]").collect(Collectors.joining(", ")) + ", " +
                "t.LastModifiedAt = GETDATE() " +
                "WHEN NOT MATCHED THEN " +
                "INSERT (" + String.join(", ", columns) + ", CreatedAt, LastModifiedAt) " +
                "VALUES (" + columns.stream().map(c -> "s.[" + c + "]").collect(Collectors.joining(", ")) + ", GETDATE(), NULL); " +

                "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " OFF;";
    }
}