package org.wesuper.jtools.hdscompare.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.CollectionUtils;

import java.io.InputStream;
import java.util.*;

/**
 * 异构数据源配置类
 * 
 * @author vincentruan
 * @version 1.0.0
 */
@Configuration
@ConfigurationProperties(prefix = "jtools.hdscompare.config")
public class DataSourceCompareConfig implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceCompareConfig.class);

    /**
     * 表结构比对配置 (通过 application.yml 等 Spring 环境加载)
     */
    private List<CompareConfig> compareConfigs = new ArrayList<>();

    /**
     * 是否在应用启动时自动执行比对
     */
    private boolean autoCompareOnStartup = true;

    /**
     * 是否输出详细的比对信息到日志
     */
    private boolean verboseOutput = true;

    /**
     * 是否启用Markdown文件输出比对结果
     */
    private boolean enableMarkdownOutput = false;

    /**
     * Markdown文件输出路径
     */
    private String markdownOutputFilePath = "compare-results.md";

    /**
     * 是否从外部 YAML 文件加载额外的比对配置
     */
    private boolean loadFromExternalYaml = true;

    /**
     * 外部 YAML 比对配置文件名 (classpath)
     */
    private String externalYamlConfigFile = "hdscompare-config.yml";

    /**
     * 是否从外部 JSON 文件加载额外的比对配置
     */
    private boolean loadFromExternalJson = true;

    /**
     * 外部 JSON 比对配置文件名 (classpath)
     */
    private String externalJsonConfigFile = "hdscompare-config.json";

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

    public boolean isEnableMarkdownOutput() {
        return enableMarkdownOutput;
    }

    public void setEnableMarkdownOutput(boolean enableMarkdownOutput) {
        this.enableMarkdownOutput = enableMarkdownOutput;
    }

    public String getMarkdownOutputFilePath() {
        return markdownOutputFilePath;
    }

    public void setMarkdownOutputFilePath(String markdownOutputFilePath) {
        this.markdownOutputFilePath = markdownOutputFilePath;
    }

    public boolean isLoadFromExternalYaml() {
        return loadFromExternalYaml;
    }

    public void setLoadFromExternalYaml(boolean loadFromExternalYaml) {
        this.loadFromExternalYaml = loadFromExternalYaml;
    }

    public String getExternalYamlConfigFile() {
        return externalYamlConfigFile;
    }

    public void setExternalYamlConfigFile(String externalYamlConfigFile) {
        this.externalYamlConfigFile = externalYamlConfigFile;
    }

    public boolean isLoadFromExternalJson() {
        return loadFromExternalJson;
    }

    public void setLoadFromExternalJson(boolean loadFromExternalJson) {
        this.loadFromExternalJson = loadFromExternalJson;
    }

    public String getExternalJsonConfigFile() {
        return externalJsonConfigFile;
    }

    public void setExternalJsonConfigFile(String externalJsonConfigFile) {
        this.externalJsonConfigFile = externalJsonConfigFile;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("Starting to load and merge HDS Compare configurations...");
        Map<String, CompareConfig> finalConfigsMap = new LinkedHashMap<>();

        // 1. Load from Spring environment (e.g., application.yml) - Highest priority for existing names
        if (!CollectionUtils.isEmpty(this.compareConfigs)) {
            logger.info("Loading {} compare configs from Spring environment.", this.compareConfigs.size());
            this.compareConfigs.forEach(config -> finalConfigsMap.put(config.getName(), config));
        }

        boolean externalConfigLoadedSuccessfully = false;

        // 2. Try to load from external YAML file (higher priority external file)
        if (loadFromExternalYaml) {
            List<CompareConfig> yamlConfigs = loadConfigsFromFile(externalYamlConfigFile, ConfigFileType.YAML);
            if (!CollectionUtils.isEmpty(yamlConfigs)) {
                logger.info("Loaded {} compare configs from YAML file: {}. Merging with lower priority than Spring env.", yamlConfigs.size(), externalYamlConfigFile);
                yamlConfigs.forEach(config -> finalConfigsMap.putIfAbsent(config.getName(), config));
                externalConfigLoadedSuccessfully = true;
                logger.info("YAML config loaded. Will skip JSON config file if loadFromExternalJson is also true and it was not already skipped.");
            }
        }

        // 3. Try to load from external JSON file (only if YAML was not loaded or not enabled)
        if (loadFromExternalJson && !externalConfigLoadedSuccessfully) {
            List<CompareConfig> jsonConfigs = loadConfigsFromFile(externalJsonConfigFile, ConfigFileType.JSON);
            if (!CollectionUtils.isEmpty(jsonConfigs)) {
                logger.info("Loaded {} compare configs from JSON file: {}. Merging with lower priority than Spring env and YAML.", jsonConfigs.size(), externalJsonConfigFile);
                jsonConfigs.forEach(config -> finalConfigsMap.putIfAbsent(config.getName(), config));
                // externalConfigLoadedSuccessfully = true; // Set if JSON is the one successfully loaded
            }
        }
        
        this.compareConfigs = new ArrayList<>(finalConfigsMap.values());
        logger.info("Finished loading HDS Compare configurations. Total effective compare configs: {}",
                this.compareConfigs.size());
        if (logger.isDebugEnabled() && !this.compareConfigs.isEmpty()) {
            this.compareConfigs.forEach(
                    c -> logger.debug("Effective CompareConfig: Name='{}', Source='{}', Target='{}', TablesCount={}",
                            c.getName(), c.getSourceDataSource().getType(), c.getTargetDataSource().getType(),
                            c.getTableConfigs().size()));
        }
    }

    private enum ConfigFileType {
        JSON, YAML
    }

    private List<CompareConfig> loadConfigsFromFile(String fileName, ConfigFileType type) {
        ObjectMapper objectMapper;
        if (type == ConfigFileType.YAML) {
            objectMapper = new ObjectMapper(new YAMLFactory());
        } else {
            objectMapper = new ObjectMapper();
        }
        objectMapper.findAndRegisterModules(); // Important for Java 8 Date/Time types, etc.

        ClassPathResource resource = new ClassPathResource(fileName);
        if (!resource.exists()) {
            logger.info("External compare config file not found on classpath: {}", fileName);
            return Collections.emptyList();
        }

        try (InputStream inputStream = resource.getInputStream()) {
            return objectMapper.readValue(inputStream, new TypeReference<List<CompareConfig>>() {
            });
        } catch (Exception e) {
            logger.warn("Failed to load compare configs from file: {}. Error: {}", fileName, e.getMessage(), e);
            return Collections.emptyList();
        }
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
         * 源数据源配置
         */
        private DataSourceConfig sourceDataSource;

        /**
         * 目标数据源配置
         */
        private DataSourceConfig targetDataSource;

        /**
         * 表比对配置列表
         */
        private List<TableCompareConfig> tableConfigs = new ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DataSourceConfig getSourceDataSource() {
            return sourceDataSource;
        }

        public void setSourceDataSource(DataSourceConfig sourceDataSource) {
            this.sourceDataSource = sourceDataSource;
        }

        public DataSourceConfig getTargetDataSource() {
            return targetDataSource;
        }

        public void setTargetDataSource(DataSourceConfig targetDataSource) {
            this.targetDataSource = targetDataSource;
        }

        public List<TableCompareConfig> getTableConfigs() {
            return tableConfigs;
        }

        public void setTableConfigs(List<TableCompareConfig> tableConfigs) {
            this.tableConfigs = tableConfigs;
        }
    }

    /**
     * 数据源配置
     */
    public static class DataSourceConfig {
        /**
         * 数据源类型: mysql, tidb, elasticsearch等
         */
        private String type;

        /**
         * 数据源名称，要与Spring中配置的数据源名称一致
         */
        private String dataSourceName;

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

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }

    /**
     * 表比对配置
     */
    public static class TableCompareConfig {
        /**
         * 源表名
         */
        private String sourceTableName;

        /**
         * 目标表名
         */
        private String targetTableName;

        /**
         * 忽略的字段列表
         */
        private List<String> ignoreFields = new ArrayList<>();

        /**
         * 忽略的比对类型，如 INDEX、COMMENT 等
         */
        private List<String> ignoreTypes = new ArrayList<>();

        public String getSourceTableName() {
            return sourceTableName;
        }

        public void setSourceTableName(String sourceTableName) {
            this.sourceTableName = sourceTableName;
        }

        public String getTargetTableName() {
            return targetTableName;
        }

        public void setTargetTableName(String targetTableName) {
            this.targetTableName = targetTableName;
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
}