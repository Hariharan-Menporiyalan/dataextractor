package com.larsentoubro.dataextractor.jsondata;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.List;

@Setter
@Getter
@Component
public class TableConfig {
    private String sourceDatabase;

    private List<TableMapping> tablesForChanges;
}
