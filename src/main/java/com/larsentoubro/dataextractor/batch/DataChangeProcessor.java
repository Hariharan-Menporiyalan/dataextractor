package com.larsentoubro.dataextractor.batch;

import jakarta.annotation.PostConstruct;
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
    private Set<Map<String, Object>> targetRecords;
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

    @PostConstruct
    public void loadTargetData() {
        String selectAllSql = "SELECT * FROM " + targetSchema + "." + targetTable;
        List<Map<String, Object>> records = jdbcTemplate.queryForList(selectAllSql);
        log.info("Loaded target table data of size {} into memory...", records.size());

        // Create a Map for fast lookups based on primary keys
        targetRecords = new HashSet<>();
        for (Map<String, Object> record : records) {
            Map<String, Object> primaryKeyMap = primaryKeys.stream()
                    .collect(Collectors.toMap(pk -> pk, record::get));
            targetRecords.add(primaryKeyMap);
        }
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) {
        if (item == null || item.isEmpty()) return null;

        Map<String, Object> primaryKeyMap = primaryKeys.stream()
                .collect(Collectors.toMap(pk -> pk, item::get));

        processedPrimaryKeys.add(primaryKeyMap);

        if (targetRecords.contains(primaryKeyMap)) {
            Map<String, Object> existingRecord = findRecord(primaryKeyMap);
            if (existingRecord != null && hasChanges(existingRecord, item)) {
                return item; // Update
            } else {
                return null; // No changes
            }
        } else {
            return item; // Insert
        }
    }

    private Map<String, Object> findRecord(Map<String, Object> primaryKeyMap) {
        return targetRecords.stream()
                .filter(record -> primaryKeys.stream().allMatch(pk -> Objects.equals(record.get(pk), primaryKeyMap.get(pk))))
                .findFirst()
                .orElse(null);
    }

    private boolean hasChanges(Map<String, Object> existingRecord, Map<String, Object> newRecord) {
        return newRecord.entrySet().stream()
                .filter(entry -> !primaryKeys.contains(entry.getKey()))
                .anyMatch(entry -> !Objects.equals(entry.getValue(), existingRecord.get(entry.getKey())));
    }

    @AfterStep
    public void cleanup(StepExecution stepExecution) {
        log.info("Performing cleanup: Deletes...");

        Set<Map<String, Object>> targetPrimaryKeys = targetRecords.stream()
                .map(record -> primaryKeys.stream().collect(Collectors.toMap(pk -> pk, record::get)))
                .collect(Collectors.toSet());

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