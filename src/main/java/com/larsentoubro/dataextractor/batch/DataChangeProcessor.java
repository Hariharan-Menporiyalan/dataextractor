package com.larsentoubro.dataextractor.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
@StepScope
public class DataChangeProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    private final JdbcTemplate jdbcTemplate;
    private final String targetSchema;
    private final String targetTable;
    private final List<String> primaryKeys;
    private final Set<Map<String, Object>> processedPrimaryKeys = ConcurrentHashMap.newKeySet();

    @Autowired
    public DataChangeProcessor(@Value("#{jobParameters['targetSchema']}") String targetSchema,
                               @Value("#{jobParameters['targetTable']}") String targetTable,
                               @Value("#{jobParameters['primaryKeys']}") String primaryKeysCsv,
                               @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.primaryKeys = Arrays.asList(primaryKeysCsv.split(","));
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) {
        if (item.isEmpty()) return null;

        // Extract primary key values
        Map<String, Object> primaryKeyMap = primaryKeys.stream()
                .collect(Collectors.toMap(pk -> pk, item::get));

        processedPrimaryKeys.add(primaryKeyMap);

        // Query the target table for existing records
        String whereClause = primaryKeys.stream().map(pk -> pk + " = ?").collect(Collectors.joining(" AND "));
        String sql = "SELECT * FROM " + targetSchema + "." + targetTable + " WHERE " + whereClause;
        Object[] primaryKeyValues = primaryKeys.stream().map(item::get).toArray();
        List<Map<String, Object>> targetRecords = jdbcTemplate.queryForList(sql, primaryKeyValues);

        if (targetRecords.isEmpty()) {
            return item;
        }

        Map<String, Object> existingRecord = targetRecords.get(0);
        if (hasChanges(existingRecord, item)) {
            return item;
        }

        return null;
    }

    private boolean hasChanges(Map<String, Object> existingRecord, Map<String, Object> newRecord) {
        return newRecord.entrySet().stream()
                .filter(entry -> !primaryKeys.contains(entry.getKey())) // Exclude primary keys
                .anyMatch(entry -> !Objects.equals(entry.getValue(), existingRecord.get(entry.getKey())));
    }

    @AfterStep
    public void cleanup(StepExecution stepExecution) {
        log.info("Performing cleanup to remove deleted records...");

        // Fetch all records from the target table
        String selectAllSql = "SELECT * FROM " + targetSchema + "." + targetTable;
        List<Map<String, Object>> targetRecords = jdbcTemplate.queryForList(selectAllSql);

        for (Map<String, Object> targetRecord : targetRecords) {
            Map<String, Object> primaryKeyMap = primaryKeys.stream()
                    .collect(Collectors.toMap(pk -> pk, targetRecord::get));

            if (!processedPrimaryKeys.contains(primaryKeyMap)) {
                String deleteSql = "DELETE FROM " + targetSchema + "." + targetTable + " WHERE " +
                        primaryKeys.stream().map(pk -> pk + " = ?").collect(Collectors.joining(" AND "));

                jdbcTemplate.update(deleteSql, primaryKeys.stream().map(targetRecord::get).toArray());
                log.info("Deleted record from {}.{} where {}", targetSchema, targetTable, primaryKeyMap);
            }
        }
    }
}
