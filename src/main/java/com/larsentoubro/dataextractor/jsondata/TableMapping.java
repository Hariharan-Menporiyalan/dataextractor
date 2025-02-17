package com.larsentoubro.dataextractor.jsondata;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.List;

@Setter
@Getter
@Component
public class TableMapping {

    private String sourceSchema;

    private String sourceTable;

    private List<String> primaryKey;

    private String targetSchema;

    private String targetTable;
}
