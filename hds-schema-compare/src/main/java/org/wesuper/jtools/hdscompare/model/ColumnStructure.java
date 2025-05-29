package org.wesuper.jtools.hdscompare.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 字段结构模型，用于统一描述不同数据源的字段结构
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class ColumnStructure {
    
    private static final Logger logger = LoggerFactory.getLogger(ColumnStructure.class);
    
    /**
     * 字段名
     */
    private String columnName;
    
    /**
     * 字段数据类型
     */
    private String dataType;
    
    /**
     * 字段类型（完整定义，如VARCHAR(255)）
     */
    private String columnType;
    
    /**
     * 字段长度
     */
    private Integer length;
    
    /**
     * 精度（用于浮点类型）
     */
    private Integer precision;
    
    /**
     * 小数位数
     */
    private Integer scale;
    
    /**
     * 是否允许为空
     */
    private boolean nullable;
    
    /**
     * 默认值
     */
    private String defaultValue;
    
    /**
     * 是否自增
     */
    private boolean autoIncrement;
    
    /**
     * 字段注释
     */
    private String comment;
    
    /**
     * 字段在表中的顺序
     */
    private Integer ordinalPosition;
    
    /**
     * 字段的附加属性，用于存储特定数据源的额外信息
     */
    private Map<String, Object> properties = new HashMap<>();
    
    // 支持多类型映射
    private List<TypeMapping> typeMappings;

    public ColumnStructure() {
        this.typeMappings = new ArrayList<>();
    }

    // 类型映射内部类
    public static class TypeMapping {
        private String targetType;  // 目标数据库类型
        private List<String> columnTypes;  // 列类型列表

        public TypeMapping() {
            this.columnTypes = new ArrayList<>();
        }

        public String getTargetType() {
            return targetType;
        }

        public void setTargetType(String targetType) {
            this.targetType = targetType;
        }

        public List<String> getColumnTypes() {
            return columnTypes;
        }

        public void setColumnTypes(List<String> columnTypes) {
            this.columnTypes = columnTypes;
        }

        public void addColumnType(String columnType) {
            if (!this.columnTypes.contains(columnType)) {
                this.columnTypes.add(columnType);
            }
        }

        public boolean hasColumnType(String columnType) {
            return this.columnTypes.contains(columnType);
        }

        public boolean hasCommonColumnType(TypeMapping other) {
            return this.columnTypes.stream()
                    .anyMatch(type -> other.columnTypes.contains(type));
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(Integer ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public List<TypeMapping> getTypeMappings() {
        return typeMappings;
    }

    public void setTypeMappings(List<TypeMapping> typeMappings) {
        this.typeMappings = typeMappings;
    }

    // 添加类型映射的便捷方法
    public void addTypeMapping(String targetType, String columnType) {
        TypeMapping mapping = getTypeMapping(targetType);
        if (mapping == null) {
            mapping = new TypeMapping();
            mapping.setTargetType(targetType);
            this.typeMappings.add(mapping);
        }
        mapping.addColumnType(columnType);
    }

    // 获取特定目标类型的映射
    public TypeMapping getTypeMapping(String targetType) {
        logger.debug("Column '{}': getTypeMapping called for targetType: '{}'", this.columnName, targetType);
        if (this.typeMappings == null) {
            logger.debug("Column '{}': this.typeMappings is NULL", this.columnName);
            return null;
        }
        logger.debug("Column '{}': Examining {} stored mappings:", this.columnName, this.typeMappings.size());
        for (TypeMapping tm : this.typeMappings) {
            logger.debug("  - Stored Mapping for column '{}': target='{}', types={}", this.columnName, tm.getTargetType(), tm.getColumnTypes());
        }

        TypeMapping foundMapping = typeMappings.stream()
                .filter(mapping -> {
                    // Robust comparison for targetType
                    String storedTargetType = mapping.getTargetType();
                    if (targetType == null) {
                        return storedTargetType == null;
                    } else {
                        return targetType.equals(storedTargetType);
                    }
                })
                .findFirst()
                .orElse(null);
        
        if (foundMapping == null) {
            logger.debug("Column '{}': No mapping found for targetType: '{}' after stream filter.", this.columnName, targetType);
        } else {
            logger.debug("Column '{}': Found mapping for targetType: '{}' -> {}", this.columnName, targetType, foundMapping.getColumnTypes());
        }
        return foundMapping;
    }

    // 检查与另一个ColumnStructure的类型兼容性
    public boolean isTypeCompatible(ColumnStructure other, String targetType) {
        TypeMapping thisMapping = this.getTypeMapping(targetType);
        TypeMapping otherMapping = other.getTypeMapping(targetType);
        
        if (thisMapping == null || otherMapping == null) {
            return false;
        }
        
        return thisMapping.hasCommonColumnType(otherMapping);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnStructure that = (ColumnStructure) o;
        return Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName);
    }

    @Override
    public String toString() {
        return "ColumnStructure{" +
                "columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", columnType='" + columnType + '\'' +
                ", nullable=" + nullable +
                ", defaultValue='" + defaultValue + '\'' +
                ", autoIncrement=" + autoIncrement +
                '}';
    }
} 