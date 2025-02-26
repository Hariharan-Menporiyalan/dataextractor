package com.larsentoubro.dataextractor.batch;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
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
    private final Set<String> primaryKeys; // Use a Set for primary keys
    private Map<Map<String, Object>, Map<String, Object>> targetRecordsByPrimaryKey;
    private final Set<Map<String, Object>> processedPrimaryKeys = ConcurrentHashMap.newKeySet();

    @Autowired
    public DataChangeProcessor(@Value("#{jobParameters['targetSchema']}") String targetSchema,
                               @Value("#{jobParameters['targetTable']}") String targetTable,
                               @Value("#{jobParameters['primaryKeys']}") String primaryKeysCsv,
                               @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.primaryKeys = Set.of(primaryKeysCsv.split(",")); // Use Set.of for immutability
    }

    @PostConstruct
    public void loadTargetData() {
        String selectAllSql = "SELECT * FROM " + targetSchema + "." + targetTable;
        List<Map<String, Object>> records = jdbcTemplate.queryForList(selectAllSql);
        log.info("Loaded target table data of size {} into memory", records.size());

        targetRecordsByPrimaryKey = new ConcurrentHashMap<>();
        for (Map<String, Object> record : records) {
            Map<String, Object> primaryKeyMap = primaryKeys.stream()
                    .collect(Collectors.toMap(pk -> pk, record::get));
            targetRecordsByPrimaryKey.put(primaryKeyMap, record); // Key: Primary Key Map, Value: Full Record
        }
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) {
        if (item == null || item.isEmpty()) return null;

        Map<String, Object> primaryKeyMap = primaryKeys.stream()
                .collect(Collectors.toMap(pk -> pk, item::get));

        processedPrimaryKeys.add(primaryKeyMap);

        Map<String, Object> existingRecord = targetRecordsByPrimaryKey.get(primaryKeyMap);

        if (existingRecord != null) {
            if (hasChanges(existingRecord, item)) {
                return item; // Update
            } else {
                return null; // No changes
            }
        } else {
            return item; // Insert
        }
    }

    private boolean hasChanges(Map<String, Object> existingRecord, Map<String, Object> newRecord) {
        if (existingRecord == null) return true;

        for (String key : newRecord.keySet()) {
            if (!primaryKeys.contains(key)) {
                Object newValue = newRecord.get(key);
                Object existingValue = existingRecord.get(key);
                if (!Objects.equals(newValue, existingValue)) {
                    return true; // Change detected
                }
            }
        }
        return false; // No changes detected
    }

    @AfterStep
    public void cleanup(StepExecution stepExecution) {
        if (stepExecution.getStatus().equals(BatchStatus.COMPLETED)) {
            log.info("Successfully captured change data from {}.{}", targetSchema, targetTable);
            log.info("Performing cleanup: Deletes...");

            Set<Map<String, Object>> targetPrimaryKeys = targetRecordsByPrimaryKey.keySet();

            targetPrimaryKeys.removeAll(processedPrimaryKeys);

            if (!targetPrimaryKeys.isEmpty()) {
                String deleteSql = "DELETE FROM " + targetSchema + "." + targetTable + " WHERE " +
                        primaryKeys.stream().map(pk -> pk + " = ?").collect(Collectors.joining(" AND "));

                jdbcTemplate.batchUpdate(deleteSql, targetPrimaryKeys.stream()
                        .map(pkMap -> primaryKeys.stream().map(pkMap::get).toArray())
                        .collect(Collectors.toList()));

                log.info("Deleted {} records.", targetPrimaryKeys.size());
            }
        }
    }
}