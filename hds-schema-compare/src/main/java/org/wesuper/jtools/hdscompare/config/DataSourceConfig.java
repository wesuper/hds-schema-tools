package org.wesuper.jtools.hdscompare.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * 异构数据源配置类
 * 
 * @author vincentruan
 * @version 1.0.0
 */
@Configuration
@ConfigurationProperties(prefix = "jtools.hdscompare.datasource")
public class DataSourceConfig {

    /**
     * 表结构比对配置
     */
    private List<CompareConfig> compareConfigs = new ArrayList<>();
    
    /**
     * 是否在应用启动时自动执行比对
     */
    private boolean autoCompareOnStartup = true;
    
    /**
     * 是否输出详细的比对信息
     */
    private boolean verboseOutput = true;

    public List<CompareConfig> getCompareConfigs() {
        return compareConfigs;
    }

    public void setCompareConfigs(List<CompareConfig> compareConfigs) {
        this.compareConfigs = compareConfigs;
    }

    public boolean isAutoCompareOnStartup() {
        return autoCompareOnStartup;
    }

    public void setAutoCompareOnStartup(boolean autoCompareOnStartup) {
        this.autoCompareOnStartup = autoCompareOnStartup;
    }

    public boolean isVerboseOutput() {
        return verboseOutput;
    }

    public void setVerboseOutput(boolean verboseOutput) {
        this.verboseOutput = verboseOutput;
    }

    /**
     * 单个比对配置项
     */
    public static class CompareConfig {
        /**
         * 比对名称，用于在日志中区分不同的比对任务
         */
        private String name;
        
        /**
         * 源表配置
         */
        private TableConfig sourceTable;
        
        /**
         * 目标表配置
         */
        private TableConfig targetTable;
        
        /**
         * 忽略的字段列表
         */
        private List<String> ignoreFields = new ArrayList<>();
        
        /**
         * 忽略的比对类型，如 INDEX、COMMENT 等
         */
        private List<String> ignoreTypes = new ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public TableConfig getSourceTable() {
            return sourceTable;
        }

        public void setSourceTable(TableConfig sourceTable) {
            this.sourceTable = sourceTable;
        }

        public TableConfig getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(TableConfig targetTable) {
            this.targetTable = targetTable;
        }

        public List<String> getIgnoreFields() {
            return ignoreFields;
        }

        public void setIgnoreFields(List<String> ignoreFields) {
            this.ignoreFields = ignoreFields;
        }

        public List<String> getIgnoreTypes() {
            return ignoreTypes;
        }

        public void setIgnoreTypes(List<String> ignoreTypes) {
            this.ignoreTypes = ignoreTypes;
        }
    }

    /**
     * 表配置
     */
    public static class TableConfig {
        /**
         * 数据源类型: mysql, tidb, elasticsearch等
         */
        private String type;
        
        /**
         * 数据源名称，要与Spring中配置的数据源名称一致
         */
        private String dataSourceName;
        
        /**
         * 表名或索引名
         */
        private String tableName;
        
        /**
         * 扩展属性，用于不同数据源类型的特殊配置
         */
        private Map<String, String> properties = new LinkedHashMap<>();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getDataSourceName() {
            return dataSourceName;
        }

        public void setDataSourceName(String dataSourceName) {
            this.dataSourceName = dataSourceName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }
} 