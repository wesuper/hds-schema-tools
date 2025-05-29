package org.wesuper.jtools.hdscompare.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表结构比对结果模型
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class CompareResult {
    
    /**
     * 比对任务名称
     */
    private String name;
    
    /**
     * 源表结构
     */
    private TableStructure sourceTable;
    
    /**
     * 目标表结构
     */
    private TableStructure targetTable;
    
    /**
     * 是否完全匹配
     */
    private boolean isFullyMatched;
    
    /**
     * 总匹配度（0-100%）
     */
    private double matchPercentage;
    
    /**
     * 列结构差异
     */
    private List<ColumnDifference> columnDifferences = new ArrayList<>();
    
    /**
     * 索引结构差异
     */
    private List<IndexDifference> indexDifferences = new ArrayList<>();
    
    /**
     * 表级差异
     */
    private List<TableDifference> tableDifferences = new ArrayList<>();
    
    /**
     * 分类后的差异统计
     */
    private Map<DifferenceLevel, Integer> differenceCountByLevel = new HashMap<>();

    public CompareResult() {
        this.columnDifferences = new ArrayList<>();
        this.indexDifferences = new ArrayList<>();
        this.tableDifferences = new ArrayList<>();
        this.differenceCountByLevel = new HashMap<>();
    }

    public CompareResult(String name) {
        this();
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TableStructure getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(TableStructure sourceTable) {
        this.sourceTable = sourceTable;
    }

    public TableStructure getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(TableStructure targetTable) {
        this.targetTable = targetTable;
    }

    public boolean isFullyMatched() {
        return isFullyMatched;
    }

    public void setFullyMatched(boolean fullyMatched) {
        isFullyMatched = fullyMatched;
    }

    public double getMatchPercentage() {
        return matchPercentage;
    }

    public void setMatchPercentage(double matchPercentage) {
        this.matchPercentage = matchPercentage;
    }

    public List<ColumnDifference> getColumnDifferences() {
        return columnDifferences;
    }

    public void setColumnDifferences(List<ColumnDifference> columnDifferences) {
        this.columnDifferences = columnDifferences;
    }

    public List<IndexDifference> getIndexDifferences() {
        return indexDifferences;
    }

    public void setIndexDifferences(List<IndexDifference> indexDifferences) {
        this.indexDifferences = indexDifferences;
    }

    public List<TableDifference> getTableDifferences() {
        return tableDifferences;
    }

    public void setTableDifferences(List<TableDifference> tableDifferences) {
        this.tableDifferences = tableDifferences;
    }

    public Map<DifferenceLevel, Integer> getDifferenceCountByLevel() {
        return differenceCountByLevel;
    }

    public void setDifferenceCountByLevel(Map<DifferenceLevel, Integer> differenceCountByLevel) {
        this.differenceCountByLevel = differenceCountByLevel;
    }
    
    /**
     * 增加特定级别的差异计数
     * 
     * @param level 差异级别
     */
    public void incrementDifferenceCount(DifferenceLevel level) {
        differenceCountByLevel.put(level, differenceCountByLevel.getOrDefault(level, 0) + 1);
    }
    
    /**
     * 检查是否存在严重级别的差异
     * 
     * @return 是否存在严重差异
     */
    public boolean hasCriticalDifferences() {
        return differenceCountByLevel.getOrDefault(DifferenceLevel.CRITICAL, 0) > 0;
    }
    
    /**
     * 检查是否存在警告级别的差异
     * 
     * @return 是否存在警告差异
     */
    public boolean hasWarningDifferences() {
        return differenceCountByLevel.getOrDefault(DifferenceLevel.WARNING, 0) > 0;
    }
    
    /**
     * 差异详情基类
     */
    public static abstract class Difference {
        /**
         * 差异类型
         */
        private DifferenceType type;
        
        /**
         * 差异级别
         */
        private DifferenceLevel level;
        
        /**
         * 差异描述
         */
        private String description;
        
        public Difference(DifferenceType type, DifferenceLevel level, String description) {
            this.type = type;
            this.level = level;
            this.description = description;
        }

        public DifferenceType getType() {
            return type;
        }

        public void setType(DifferenceType type) {
            this.type = type;
        }

        public DifferenceLevel getLevel() {
            return level;
        }

        public void setLevel(DifferenceLevel level) {
            this.level = level;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }
    
    /**
     * 字段差异
     */
    public static class ColumnDifference extends Difference {
        /**
         * 字段名称
         */
        private String columnName;
        
        /**
         * 源字段结构
         */
        private ColumnStructure sourceColumn;
        
        /**
         * 目标字段结构
         */
        private ColumnStructure targetColumn;
        
        /**
         * 具体的属性差异
         */
        private Map<String, PropertyDifference> propertyDifferences = new HashMap<>();
        
        public ColumnDifference(DifferenceType type, DifferenceLevel level, String description, String columnName) {
            super(type, level, description);
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public ColumnStructure getSourceColumn() {
            return sourceColumn;
        }

        public void setSourceColumn(ColumnStructure sourceColumn) {
            this.sourceColumn = sourceColumn;
        }

        public ColumnStructure getTargetColumn() {
            return targetColumn;
        }

        public void setTargetColumn(ColumnStructure targetColumn) {
            this.targetColumn = targetColumn;
        }

        public Map<String, PropertyDifference> getPropertyDifferences() {
            return propertyDifferences;
        }

        public void setPropertyDifferences(Map<String, PropertyDifference> propertyDifferences) {
            this.propertyDifferences = propertyDifferences;
        }
        
        /**
         * 添加属性差异
         * 
         * @param property 属性名
         * @param sourceValue 源值
         * @param targetValue 目标值
         * @param level 差异级别
         */
        public void addPropertyDifference(String property, Object sourceValue, Object targetValue, DifferenceLevel level) {
            PropertyDifference diff = new PropertyDifference(property, sourceValue, targetValue, level);
            propertyDifferences.put(property, diff);
        }
    }
    
    /**
     * 索引差异
     */
    public static class IndexDifference extends Difference {
        /**
         * 索引名称
         */
        private String indexName;
        
        /**
         * 源索引结构
         */
        private IndexStructure sourceIndex;
        
        /**
         * 目标索引结构
         */
        private IndexStructure targetIndex;
        
        /**
         * 具体的属性差异
         */
        private Map<String, PropertyDifference> propertyDifferences = new HashMap<>();
        
        public IndexDifference(DifferenceType type, DifferenceLevel level, String description, String indexName) {
            super(type, level, description);
            this.indexName = indexName;
        }

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        public IndexStructure getSourceIndex() {
            return sourceIndex;
        }

        public void setSourceIndex(IndexStructure sourceIndex) {
            this.sourceIndex = sourceIndex;
        }

        public IndexStructure getTargetIndex() {
            return targetIndex;
        }

        public void setTargetIndex(IndexStructure targetIndex) {
            this.targetIndex = targetIndex;
        }

        public Map<String, PropertyDifference> getPropertyDifferences() {
            return propertyDifferences;
        }

        public void setPropertyDifferences(Map<String, PropertyDifference> propertyDifferences) {
            this.propertyDifferences = propertyDifferences;
        }
        
        /**
         * 添加属性差异
         * 
         * @param property 属性名
         * @param sourceValue 源值
         * @param targetValue 目标值
         * @param level 差异级别
         */
        public void addPropertyDifference(String property, Object sourceValue, Object targetValue, DifferenceLevel level) {
            PropertyDifference diff = new PropertyDifference(property, sourceValue, targetValue, level);
            propertyDifferences.put(property, diff);
        }
    }
    
    /**
     * 表级差异
     */
    public static class TableDifference extends Difference {
        /**
         * 表属性名称
         */
        private String propertyName;
        
        /**
         * 源值
         */
        private Object sourceValue;
        
        /**
         * 目标值
         */
        private Object targetValue;
        
        public TableDifference(DifferenceType type, DifferenceLevel level, String description, String propertyName, 
                               Object sourceValue, Object targetValue) {
            super(type, level, description);
            this.propertyName = propertyName;
            this.sourceValue = sourceValue;
            this.targetValue = targetValue;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public void setPropertyName(String propertyName) {
            this.propertyName = propertyName;
        }

        public Object getSourceValue() {
            return sourceValue;
        }

        public void setSourceValue(Object sourceValue) {
            this.sourceValue = sourceValue;
        }

        public Object getTargetValue() {
            return targetValue;
        }

        public void setTargetValue(Object targetValue) {
            this.targetValue = targetValue;
        }
    }
    
    /**
     * 属性差异
     */
    public static class PropertyDifference {
        /**
         * 属性名
         */
        private String property;
        
        /**
         * 源值
         */
        private Object sourceValue;
        
        /**
         * 目标值
         */
        private Object targetValue;
        
        /**
         * 差异级别
         */
        private DifferenceLevel level;
        
        public PropertyDifference(String property, Object sourceValue, Object targetValue, DifferenceLevel level) {
            this.property = property;
            this.sourceValue = sourceValue;
            this.targetValue = targetValue;
            this.level = level;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }

        public Object getSourceValue() {
            return sourceValue;
        }

        public void setSourceValue(Object sourceValue) {
            this.sourceValue = sourceValue;
        }

        public Object getTargetValue() {
            return targetValue;
        }

        public void setTargetValue(Object targetValue) {
            this.targetValue = targetValue;
        }

        public DifferenceLevel getLevel() {
            return level;
        }

        public void setLevel(DifferenceLevel level) {
            this.level = level;
        }
    }
    
    /**
     * 差异类型枚举
     */
    public enum DifferenceType {
        /**
         * 列缺失
         */
        COLUMN_MISSING, 
        
        /**
         * 列类型不同
         */
        COLUMN_TYPE_DIFFERENT, 
        
        /**
         * 列属性不同
         */
        COLUMN_PROPERTY_DIFFERENT, 
        
        /**
         * 索引缺失
         */
        INDEX_MISSING, 
        
        /**
         * 索引结构不同
         */
        INDEX_STRUCTURE_DIFFERENT, 
        
        /**
         * 表属性不同
         */
        TABLE_PROPERTY_DIFFERENT
    }
    
    /**
     * 差异级别枚举
     */
    public enum DifferenceLevel {
        /**
         * 严重差异，可能导致数据同步异常
         */
        CRITICAL(3), 
        
        /**
         * 警告差异，可能导致性能问题
         */
        WARNING(2), 
        
        /**
         * 注意差异，通常不会导致问题
         */
        NOTICE(1), 
        
        /**
         * 可接受差异，可以忽略
         */
        ACCEPTABLE(0);
        
        private final int level;
        
        DifferenceLevel(int level) {
            this.level = level;
        }
        
        public int getLevel() {
            return level;
        }
    }
} 