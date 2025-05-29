package org.wesuper.jtools.hdscompare.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * 表结构模型，用于统一描述不同数据源的表结构
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class TableStructure {
    
    /**
     * 表名
     */
    private String tableName;
    
    /**
     * 数据源类型
     */
    private String sourceType;
    
    /**
     * 表注释
     */
    private String tableComment;
    
    /**
     * 表的字段列表
     */
    private List<ColumnStructure> columns = new ArrayList<>();
    
    /**
     * 表的索引列表
     */
    private List<IndexStructure> indexes = new ArrayList<>();
    
    /**
     * 表的附加属性，用于存储特定数据源的额外信息
     */
    private Map<String, Object> properties = new HashMap<>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public List<ColumnStructure> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnStructure> columns) {
        this.columns = columns;
    }

    public List<IndexStructure> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexStructure> indexes) {
        this.indexes = indexes;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
    
    /**
     * 根据字段名获取字段结构
     * 
     * @param columnName 字段名
     * @return 字段结构，如果不存在则返回null
     */
    public ColumnStructure getColumnByName(String columnName) {
        if (columnName == null || columns == null) {
            return null;
        }
        
        return columns.stream()
                .filter(column -> columnName.equalsIgnoreCase(column.getColumnName()))
                .findFirst()
                .orElse(null);
    }
    
    /**
     * 根据索引名获取索引结构
     * 
     * @param indexName 索引名
     * @return 索引结构，如果不存在则返回null
     */
    public IndexStructure getIndexByName(String indexName) {
        if (indexName == null || indexes == null) {
            return null;
        }
        
        return indexes.stream()
                .filter(index -> indexName.equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return "TableStructure{" +
                "tableName='" + tableName + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", columns=" + columns +
                ", indexes=" + indexes +
                '}';
    }
} 