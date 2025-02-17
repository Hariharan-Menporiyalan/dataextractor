package com.larsentoubro.dataextractor.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.larsentoubro.dataextractor.jsondata.TableConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Component
public class TableConfigLoader {

    private final ObjectMapper objectMapper;

    @Autowired
    public TableConfigLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<TableConfig> loadTableConfig() throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("table-update.json");
        if (inputStream == null) {
            throw new FileNotFoundException("table-update.json not found in resources folder");
        }
        return objectMapper.readValue(inputStream, new TypeReference<List<TableConfig>>() {});
    }
}


