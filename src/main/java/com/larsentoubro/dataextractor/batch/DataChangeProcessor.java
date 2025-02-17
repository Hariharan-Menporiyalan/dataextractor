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
import java.util.stream.Collectors;

@Slf4j
@Component
@StepScope
public class DataChangeProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {
    private final JdbcTemplate jdbcTemplate;
    private final String targetSchema;
    private final String targetTable;
    private final List<String> primaryKeys;
    private static final Set<Map<String, Object>> processedPrimaryKeys = Collections.synchronizedSet(new HashSet<>());

    @Autowired
    public DataChangeProcessor(@Value("#{jobParameters['targetSchema']}") String targetSchema,
                               @Value("#{jobParameters['targetTable']}") String targetTable,
                               @Value("#{jobParameters['primaryKeys']}") String primaryKeys,
                               @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.primaryKeys = Arrays.asList(primaryKeys.split(","));
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) {
        //Construct the WHERE clause for primary keys
        String whereClause = primaryKeys.stream()
                .map(pk -> pk + " = ?")
                .collect(Collectors.joining(" AND "));

        String sql = "SELECT * FROM " + targetSchema + "." + targetTable + " WHERE " + whereClause;
        List<Map<String, Object>> existingRecords = jdbcTemplate.queryForList(sql, primaryKeys.stream().map(item::get).toArray());

        //Store processed primary keys for later deletion check
        Map<String, Object> primaryKeyMap = primaryKeys.stream()
                .collect(Collectors.toMap(pk -> pk, item::get));
        processedPrimaryKeys.add(primaryKeyMap);

        //If no record exists in the target, insert the record
        if (existingRecords.isEmpty()) {
            return item;
        }

        //Compare values (excluding primary keys)
        Map<String, Object> existingRecord = existingRecords.get(0);
        boolean hasChanges = existingRecord.entrySet().stream()
                .anyMatch(entry -> {
                    String column = entry.getKey();
                    if (primaryKeys.contains(column)) {
                        return false; // Skip primary keys comparison
                    }
                    return !Objects.equals(entry.getValue(), item.get(column));
                });

        if (hasChanges) {
            return item; // Return for upsert (update)
        }

        return null; // No change, no need to upsert
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
                // Record exists in target but not in source, DELETE IT
                String whereClause = primaryKeys.stream()
                        .map(pk -> pk + " = ?")
                        .collect(Collectors.joining(" AND "));

                String deleteSql = "DELETE FROM " + targetSchema + "." + targetTable + " WHERE " + whereClause;
                jdbcTemplate.update(deleteSql, primaryKeys.stream().map(targetRecord::get).toArray());

                log.info("Deleted record from {}.{} where {}", targetSchema, targetTable, whereClause);
            }
        }
    }
}

