package org.wesuper.jtools.hdscompare.model;

import java.util.Map;
import java.util.HashMap;
import java.util.Objects;

/**
 * 字段结构模型，用于统一描述不同数据源的字段结构
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class ColumnStructure {
    
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