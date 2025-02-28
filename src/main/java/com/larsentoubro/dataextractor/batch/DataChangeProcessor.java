package com.larsentoubro.dataextractor.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
@Component
@StepScope
public class DataChangeProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    private final String targetSchema;
    private final String targetTable;
    private final List<String> primaryKeys;
    private final JdbcTemplate jdbcTemplate;

    private final ConcurrentHashMap<Map<String, Object>, Map<String, Object>> targetRecordCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<Map<String, Object>> processedPrimaryKeys = new ConcurrentSkipListSet<>(Comparator.comparing(Map::hashCode));

    private final AtomicInteger offset = new AtomicInteger(0);
    private final AtomicBoolean hasMoreTargetRecords = new AtomicBoolean(true);
    private final ReentrantLock cacheLock = new ReentrantLock();
    private boolean isTargetTableEmpty = false;

    @Autowired
    public DataChangeProcessor(
            @Value("#{jobParameters['targetSchema']}") String targetSchema,
            @Value("#{jobParameters['targetTable']}") String targetTable,
            @Value("#{jobParameters['primaryKeys']}") String primaryKeysCsv,
            @Qualifier("targetJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.primaryKeys = Arrays.asList(primaryKeysCsv.split(","));
        this.jdbcTemplate = jdbcTemplate;
    }

    @BeforeStep
    public void checkTargetTableStatus() {
        String countSql = "SELECT COUNT(1) FROM " + targetSchema + "." + targetTable;
        Integer count = jdbcTemplate.queryForObject(countSql, Integer.class);
        isTargetTableEmpty = (count == null || count == 0);

        if (!isTargetTableEmpty) {
            loadNextBatch();
        } else {
            log.info("Target table {}.{} is empty. All source records will be inserted.", targetSchema, targetTable);
        }
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) {
        if (item == null || item.isEmpty()) {
            log.debug("Skipping null or empty item.");
            return null;
        }

        // If target table is empty, directly insert all records
        if (isTargetTableEmpty) {
            return item;
        }

        Map<String, Object> primaryKeyMap = getPrimaryKeyMap(item);

        if (!processedPrimaryKeys.add(primaryKeyMap)) {
            log.debug("Record {} already processed, skipping.", primaryKeyMap);
            return null;
        }

        cacheLock.lock();
        try {
            Map<String, Object> existingRecord = targetRecordCache.get(primaryKeyMap);

            while (existingRecord == null && hasMoreTargetRecords.get()) {
                loadNextBatch();
                existingRecord = targetRecordCache.get(primaryKeyMap);
            }

            if (existingRecord != null) {
                if (hasChanges(existingRecord, item)) {
                    targetRecordCache.remove(primaryKeyMap);
                    log.debug("Changes detected for {}, updating record.", primaryKeyMap);
                    return item;
                } else {
                    targetRecordCache.remove(primaryKeyMap);
                    log.debug("No changes for {}, skipping update.", primaryKeyMap);
                    return null;
                }
            } else {
                log.debug("New record detected for {}, inserting.", primaryKeyMap);
                return item;
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private void loadNextBatch() {
        if (!hasMoreTargetRecords.get()) return;

        cacheLock.lock();
        try {
            int currentOffset = offset.get();
            String sql = "SELECT * FROM " + targetSchema + "." + targetTable +
                    " ORDER BY " + String.join(", ", primaryKeys) + " ASC " +
                    " OFFSET " + currentOffset + " ROWS FETCH NEXT 5000 ROWS ONLY";

            List<Map<String, Object>> records = jdbcTemplate.queryForList(sql);
            if (records.isEmpty()) {
                hasMoreTargetRecords.set(false);
                return;
            }

            for (Map<String, Object> record : records) {
                Map<String, Object> primaryKeyMap = getPrimaryKeyMap(record);
                targetRecordCache.put(primaryKeyMap, record);
            }

            offset.addAndGet(records.size());
            log.info("Loaded {} target records into cache (offset: {}).", records.size(), offset.get());
        } finally {
            cacheLock.unlock();
        }
    }

    private Map<String, Object> getPrimaryKeyMap(Map<String, Object> record) {
        return primaryKeys.stream().collect(Collectors.toMap(pk -> pk, record::get));
    }

    private boolean hasChanges(Map<String, Object> existingRecord, Map<String, Object> newRecord) {
        if (existingRecord == null) return true;

        // Consider `LastModifiedAt` if available
        Object existingLastModified = existingRecord.get("LastModifiedAt");
        Object newLastModified = newRecord.get("LastModifiedAt");

        if (existingLastModified != null && newLastModified != null) {
            if (Timestamp.valueOf(newLastModified.toString()).after(Timestamp.valueOf(existingLastModified.toString()))) {
                return true;
            }
        }

        // Compare non-primary key fields
        for (String key : newRecord.keySet()) {
            if (!primaryKeys.contains(key) && !"LastModifiedAt".equals(key) && !"CreatedAt".equals(key)) {
                if (!Objects.equals(newRecord.get(key), existingRecord.get(key))) {
                    return true;
                }
            }
        }
        return false;
    }

    @AfterStep
    public void cleanup(StepExecution stepExecution) {
        if (stepExecution.getStatus().equals(BatchStatus.COMPLETED)) {
            log.info("Successfully captured change data. Performing cleanup: Deleting missing records from target...");

//            if (!isTargetTableEmpty) {
//                Set<Map<String, Object>> targetPrimaryKeys = targetRecordCache.keySet();
//                if (!targetPrimaryKeys.isEmpty()) {
//                    cacheLock.lock();
//                    try {
//                        String deleteSql = "DELETE FROM " + targetSchema + "." + targetTable + " WHERE " +
//                                primaryKeys.stream().map(pk -> pk + " = ?").collect(Collectors.joining(" AND "));
//
//                        jdbcTemplate.batchUpdate(deleteSql, targetPrimaryKeys.stream()
//                                .map(pkMap -> primaryKeys.stream().map(pkMap::get).toArray())
//                                .collect(Collectors.toList()));
//
//                        log.info("Deleted {} records that were not found in source.", targetPrimaryKeys.size());
//                    } finally {
//                        cacheLock.unlock();
//                    }
//                }
//            }
        }
    }
}
