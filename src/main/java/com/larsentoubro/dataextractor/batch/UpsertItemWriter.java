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

        return "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " ON; " +

                // Step 1: Declare a table variable
                "DECLARE @SourceData TABLE ( " +
                columns.stream().map(col -> "[" + col + "] NVARCHAR(MAX)").collect(Collectors.joining(", ")) +
                "); " +

                // Step 2: Insert data into the table variable
                "INSERT INTO @SourceData (" + columns.stream().map(col -> "[" + col + "]").collect(Collectors.joining(", ")) + ") " +
                "VALUES " +
                columns.stream().map(col -> "(" + columns.stream().map(c -> "?").collect(Collectors.joining(", ")) + ")").collect(Collectors.joining(", ")) + "; " +

                // Step 3: Insert new records where no match exists
                "INSERT INTO " + targetSchema + "." + targetTable +
                " ( " + columns.stream().map(col -> "[" + col + "]").collect(Collectors.joining(", ")) + ", CreatedAt, LastModifiedAt ) " +
                "SELECT " + columns.stream().map(col -> "s.[" + col + "]").collect(Collectors.joining(", ")) + ", GETDATE(), NULL " +
                "FROM @SourceData s " +
                "WHERE NOT EXISTS (" +
                "   SELECT 1 FROM " + targetSchema + "." + targetTable + " t " +
                "   WHERE " + matchCondition +
                "); " +

                // Step 4: Insert a new row instead of updating the matched record
                "INSERT INTO " + targetSchema + "." + targetTable +
                " ( " + columns.stream().map(col -> "[" + col + "]").collect(Collectors.joining(", ")) + ", CreatedAt, LastModifiedAt ) " +
                "SELECT " + columns.stream().map(col -> "s.[" + col + "]").collect(Collectors.joining(", ")) + ", NULL, GETDATE() " +
                "FROM @SourceData s " +
                "WHERE EXISTS (" +
                "   SELECT 1 FROM " + targetSchema + "." + targetTable + " t " +
                "   WHERE " + matchCondition +
                "); " +

                "SET IDENTITY_INSERT " + targetSchema + "." + targetTable + " OFF;";
    }
}