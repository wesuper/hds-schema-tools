package org.immortal.hydra.hdstbcomp.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;

/**
 * 索引结构模型，用于统一描述不同数据源的索引结构
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class IndexStructure {
    
    /**
     * 索引名称
     */
    private String indexName;
    
    /**
     * 索引类型（PRIMARY、UNIQUE、NORMAL、FULLTEXT等）
     */
    private String indexType;
    
    /**
     * 是否是主键
     */
    private boolean isPrimary;
    
    /**
     * 是否是唯一索引
     */
    private boolean isUnique;
    
    /**
     * 索引包含的字段列表
     */
    private List<IndexColumnStructure> columns = new ArrayList<>();
    
    /**
     * 索引的附加属性，用于存储特定数据源的额外信息
     */
    private Map<String, Object> properties = new HashMap<>();

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean unique) {
        isUnique = unique;
    }

    public List<IndexColumnStructure> getColumns() {
        return columns;
    }

    public void setColumns(List<IndexColumnStructure> columns) {
        this.columns = columns;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexStructure that = (IndexStructure) o;
        return Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName);
    }

    @Override
    public String toString() {
        return "IndexStructure{" +
                "indexName='" + indexName + '\'' +
                ", indexType='" + indexType + '\'' +
                ", isPrimary=" + isPrimary +
                ", isUnique=" + isUnique +
                ", columns=" + columns +
                '}';
    }
    
    /**
     * 索引字段结构
     */
    public static class IndexColumnStructure {
        /**
         * 字段名称
         */
        private String columnName;
        
        /**
         * 索引中的顺序
         */
        private int position;
        
        /**
         * 排序方向（ASC、DESC）
         */
        private String sort;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getSort() {
            return sort;
        }

        public void setSort(String sort) {
            this.sort = sort;
        }

        @Override
        public String toString() {
            return columnName + (sort != null ? " " + sort : "");
        }
    }
} 