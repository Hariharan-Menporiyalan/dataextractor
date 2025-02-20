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
        if (item == null || item.isEmpty()) return null;

        // Extract primary key values for lookup
        Map<String, Object> primaryKeyMap = primaryKeys.stream()
                .collect(Collectors.toMap(pk -> pk, item::get));

        processedPrimaryKeys.add(primaryKeyMap); // Store primary keys for cleanup

        // Build WHERE condition for primary keys
        String whereClause = primaryKeys.stream()
                .map(pk -> pk + " = ?")
                .collect(Collectors.joining(" AND "));

        String sql = "SELECT * FROM " + targetSchema + "." + targetTable + " WHERE " + whereClause;

        // Fetch target record
        List<Map<String, Object>> targetRecords = jdbcTemplate.queryForList(sql, primaryKeys.stream()
                .map(item::get)
                .toArray());

        if (targetRecords.isEmpty()) {
            // No record exists in target → Insert
            return item;
        }

        // Compare existing record with new record
        Map<String, Object> existingRecord = targetRecords.get(0);
        if (hasChanges(existingRecord, item)) {
            return item; // Needs update
        }

        return null; // No changes, skip this item
    }

    private boolean hasChanges(Map<String, Object> existingRecord, Map<String, Object> newRecord) {
        return newRecord.entrySet().stream()
                .filter(entry -> !primaryKeys.contains(entry.getKey())) // Ignore primary keys
                .anyMatch(entry -> !Objects.equals(entry.getValue(), existingRecord.get(entry.getKey())));
    }

    @AfterStep
    public void cleanup(StepExecution stepExecution) {
        log.info("Performing cleanup to detect deleted records...");
        String selectAllSql = "SELECT * FROM " + targetSchema + "." + targetTable;
        List<Map<String, Object>> targetRecords = jdbcTemplate.queryForList(selectAllSql);

        for (Map<String, Object> targetRecord : targetRecords) {
            Map<String, Object> primaryKeyMap = primaryKeys.stream()
                    .collect(Collectors.toMap(pk -> pk, targetRecord::get));

            if (!processedPrimaryKeys.contains(primaryKeyMap)) {
                // DELETE records that exist in target but not in source
                String deleteSql = "DELETE FROM " + targetSchema + "." + targetTable + " WHERE " +
                        primaryKeys.stream().map(pk -> pk + " = ?").collect(Collectors.joining(" AND "));

                jdbcTemplate.update(deleteSql, primaryKeys.stream().map(targetRecord::get).toArray());
                log.info("Deleted record from {}.{} where {}", targetSchema, targetTable, primaryKeyMap);
            }
        }
    }
}