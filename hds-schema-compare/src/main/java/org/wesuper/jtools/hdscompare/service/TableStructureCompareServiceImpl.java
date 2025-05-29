package org.wesuper.jtools.hdscompare.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.extractor.TableStructureExtractor;
import org.wesuper.jtools.hdscompare.extractor.TableStructureExtractorFactory;
import org.wesuper.jtools.hdscompare.model.ColumnStructure;
import org.wesuper.jtools.hdscompare.model.CompareResult;
import org.wesuper.jtools.hdscompare.model.IndexStructure;
import org.wesuper.jtools.hdscompare.model.TableStructure;
import org.wesuper.jtools.hdscompare.model.CompareResult.ColumnDifference;
import org.wesuper.jtools.hdscompare.model.CompareResult.DifferenceLevel;
import org.wesuper.jtools.hdscompare.model.CompareResult.DifferenceType;
import org.wesuper.jtools.hdscompare.model.CompareResult.IndexDifference;
import org.wesuper.jtools.hdscompare.model.CompareResult.TableDifference;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.wesuper.jtools.hdscompare.constants.DatabaseType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * 表结构比对服务实现
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Service
public class TableStructureCompareServiceImpl implements TableStructureCompareService {

    private static final Logger logger = LoggerFactory.getLogger(TableStructureCompareServiceImpl.class);

    @Autowired
    private DataSourceCompareConfig dataSourceConfig;

    @Autowired
    private TableStructureExtractorFactory extractorFactory;

    // ES特有的字段列表，在MySQL中不会出现
    private static final Set<String> ES_SPECIFIC_FIELDS = new HashSet<>(Arrays.asList(
        "number_of_replicas",
        "number_of_shards",
        "creation_date",
        "uuid",
        "provided_name",
        "has_custom_analyzers",
        "analyzer",
        "search_analyzer",
        "ignore_above",
        "scaling_factor",
        "_meta",
        "_source",
        "_all",
        "_routing",
        "_parent",
        "_field_names",
        "dynamic",
        "dynamic_templates",
        "properties",
        "include_in_all",
        "copy_to",
        "fields",
        "format",
        "ignore_malformed",
        "index_options",
        "norms",
        "null_value",
        "position_increment_gap",
        "search_quote_analyzer",
        "similarity",
        "term_vector"
    ));
    
    // 静态成员区添加：
    private static final Pattern DEFAULT_PREFIX_PATTERN = Pattern.compile("^default\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern LEADING_ZERO_PATTERN = Pattern.compile("^0+([1-9])");
    private static final Pattern TRAILING_ZERO_PATTERN = Pattern.compile("\\.0+$");

    @Override
    public CompareResult compareTableStructures(TableStructure sourceTable, TableStructure targetTable,
            DataSourceCompareConfig.CompareConfig config) {
        if (sourceTable == null || targetTable == null) {
            throw new IllegalArgumentException("Source table and target table cannot be null");
        }

        logger.info("Comparing table structures: {} ({}) vs {} ({})",
                sourceTable.getTableName(), sourceTable.getSourceType(),
                targetTable.getTableName(), targetTable.getSourceType());

        CompareResult result = new CompareResult();
        result.setName(config.getName());
        result.setSourceTable(sourceTable);
        result.setTargetTable(targetTable);

        // 1. 比对表级属性
        compareTableProperties(result, config);

        // 2. 比对列结构
        compareColumns(result, config);

        // 3. 比对索引结构
        compareIndexes(result, config);

        // 4. 计算整体匹配度
        calculateMatchPercentage(result);

        if (result.getColumnDifferences().isEmpty() &&
                result.getIndexDifferences().isEmpty() &&
                result.getTableDifferences().isEmpty()) {
            result.setFullyMatched(true);
            result.setMatchPercentage(100.0);
        }

        return result;
    }

    @Override
    public List<CompareResult> compareAllConfiguredTables() {
        List<CompareResult> results = new ArrayList<>();

        List<DataSourceCompareConfig.CompareConfig> configs = dataSourceConfig.getCompareConfigs();
        if (configs == null || configs.isEmpty()) {
            logger.warn("No table compare configurations found");
            return results;
        }

        for (DataSourceCompareConfig.CompareConfig config : configs) {
            try {
                List<CompareResult> configResults = compareTablesByConfig(config);
                if (configResults != null) {
                    results.addAll(configResults);
                }
            } catch (Exception e) {
                logger.error("Failed to compare tables for config {}: {}", config.getName(), e.getMessage(), e);
            }
        }

        return results;
    }

    @Override
    public CompareResult compareTablesByName(String name) {
        if (!StringUtils.hasText(name)) {
            throw new IllegalArgumentException("Compare config name cannot be empty");
        }

        DataSourceCompareConfig.CompareConfig config = dataSourceConfig.getCompareConfigs().stream()
                .filter(c -> name.equals(c.getName()))
                .findFirst()
                .orElse(null);

        if (config == null) {
            logger.warn("No compare configuration found with name: {}", name);
            return null;
        }

        List<CompareResult> results = compareTablesByConfig(config);
        return results != null && !results.isEmpty() ? results.get(0) : null;
    }

    @Override
    public TableStructure getTableStructure(DataSourceCompareConfig.DataSourceConfig dataSourceConfig, String tableName)
            throws Exception {
        if (dataSourceConfig == null) {
            throw new IllegalArgumentException("Data source config cannot be null");
        }

        String sourceType = dataSourceConfig.getType();
        if (!StringUtils.hasText(sourceType)) {
            throw new IllegalArgumentException("Source type cannot be empty");
        }

        TableStructureExtractor extractor = extractorFactory.getExtractor(sourceType);
        if (extractor == null) {
            throw new IllegalArgumentException("No extractor found for source type: " + sourceType);
        }

        return extractor.extractTableStructure(dataSourceConfig, tableName);
    }

    /**
     * 根据比对配置比对表结构
     *
     * @param config 比对配置
     * @return 比对结果列表
     */
    private List<CompareResult> compareTablesByConfig(DataSourceCompareConfig.CompareConfig config) {
        List<CompareResult> results = new ArrayList<>();

        try {
            // 获取源数据源配置
            DataSourceCompareConfig.DataSourceConfig sourceConfig = config.getSourceDataSource();
            // 获取目标数据源配置
            DataSourceCompareConfig.DataSourceConfig targetConfig = config.getTargetDataSource();

            // 遍历每个表的比对配置
            for (DataSourceCompareConfig.TableCompareConfig tableConfig : config.getTableConfigs()) {
                try {
                    // 获取源表结构
                    TableStructure sourceTable = getTableStructure(sourceConfig, tableConfig.getSourceTableName());
                    // 获取目标表结构
                    TableStructure targetTable = getTableStructure(targetConfig, tableConfig.getTargetTableName());

                    // 创建临时配置用于比对
                    DataSourceCompareConfig.CompareConfig tempConfig = new DataSourceCompareConfig.CompareConfig();
                    tempConfig.setName(config.getName());
                    tempConfig.setSourceDataSource(sourceConfig);
                    tempConfig.setTargetDataSource(targetConfig);
                    tempConfig.setTableConfigs(Collections.singletonList(tableConfig));

                    // 比对结构
                    CompareResult result = compareTableStructures(sourceTable, targetTable, tempConfig);
                    if (result != null) {
                        results.add(result);
                    }
                } catch (Exception e) {
                    logger.error("Failed to compare tables {} vs {}: {}",
                            tableConfig.getSourceTableName(),
                            tableConfig.getTargetTableName(),
                            e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to compare tables with config {}: {}", config.getName(), e.getMessage(), e);
        }

        return results;
    }

    /**
     * 比对表级属性
     *
     * @param result 比对结果
     * @param config 比对配置
     */
    private void compareTableProperties(CompareResult result, DataSourceCompareConfig.CompareConfig config) {
        TableStructure sourceTable = result.getSourceTable();
        TableStructure targetTable = result.getTargetTable();
        
        String sourceType = sourceTable.getSourceType();
        String targetType = targetTable.getSourceType();
        
        boolean sourceIsES = sourceType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);
        boolean targetIsES = targetType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);
        boolean sourceIsPOJO = sourceType.equalsIgnoreCase(DatabaseType.POJO);
        boolean targetIsPOJO = targetType.equalsIgnoreCase(DatabaseType.POJO);

        // 检查表注释
        if (!isCommentEqual(sourceTable.getTableComment(), targetTable.getTableComment(),
                sourceType, targetType) &&
                !isIgnoredType(config, "COMMENT")) {
            TableDifference diff = new TableDifference(
                    DifferenceType.TABLE_PROPERTY_DIFFERENT,
                    DifferenceLevel.NOTICE,
                    "Table comment is different",
                    "comment",
                    sourceTable.getTableComment(),
                    targetTable.getTableComment());

            result.getTableDifferences().add(diff);
            result.incrementDifferenceCount(diff.getLevel());
        }

        // 比对表的特殊属性
        if (sourceTable.getProperties() != null && targetTable.getProperties() != null) {
            // 获取所有需要比对的属性键
            Set<String> allPropertyKeys = new HashSet<>();
            allPropertyKeys.addAll(sourceTable.getProperties().keySet());
            allPropertyKeys.addAll(targetTable.getProperties().keySet());

            for (String key : allPropertyKeys) {
                Object sourceValue = sourceTable.getProperties().get(key);
                Object targetValue = targetTable.getProperties().get(key);

                // 如果是ES特有的字段，则跳过比对
                if ((sourceIsES || targetIsES) && ES_SPECIFIC_FIELDS.contains(key)) {
                    continue;
                }

                // 对于ES到ES的比对，跳过不影响数据检索和展示的属性
                if (sourceIsES && targetIsES && isNonEssentialESProperty(key)) {
                    continue;
                }

                // 对于POJO的比对，跳过一些特定属性
                if ((sourceIsPOJO || targetIsPOJO) && isNonEssentialPOJOProperty(key)) {
                    continue;
                }

                // 对于comment属性，使用特殊的比对逻辑
                if ("comment".equalsIgnoreCase(key)) {
                    if (isCommentEqual(String.valueOf(sourceValue), String.valueOf(targetValue),
                            sourceType, targetType)) {
                        continue;
                    }
                }

                // 处理属性值差异
                if (!Objects.equals(sourceValue, targetValue)) {
                    DifferenceLevel level = getDifferenceLevel(key, config);
                    
                    // 对于ES和POJO的比对，降低某些属性的差异级别
                    if ((sourceIsES || targetIsES || sourceIsPOJO || targetIsPOJO) && 
                        isReducibleProperty(key)) {
                        level = DifferenceLevel.NOTICE;
                    }

                    TableDifference diff = new TableDifference(
                            DifferenceType.TABLE_PROPERTY_DIFFERENT,
                            level,
                            "Table property '" + key + "' is different",
                            key,
                            sourceValue,
                            targetValue);

                    result.getTableDifferences().add(diff);
                    result.incrementDifferenceCount(level);
                }
            }
        }
    }

    /**
     * 判断是否为ES到MySQL的比对
     */
    private boolean isESToMySQLComparison(String sourceType, String targetType) {
        return DatabaseType.ELASTICSEARCH.equalsIgnoreCase(sourceType) && 
               (isMySQLFamily(targetType) || DatabaseType.POJO.equalsIgnoreCase(targetType));
    }

    /**
     * 判断是否为MySQL到ES的比对
     */
    private boolean isMySQLToESComparison(String sourceType, String targetType) {
        return (isMySQLFamily(sourceType) || DatabaseType.POJO.equalsIgnoreCase(sourceType)) && 
               DatabaseType.ELASTICSEARCH.equalsIgnoreCase(targetType);
    }

    /**
     * 判断是否为ES到ES的比对
     */
    private boolean isESComparison(String sourceType, String targetType) {
        return DatabaseType.ELASTICSEARCH.equalsIgnoreCase(sourceType) && 
               DatabaseType.ELASTICSEARCH.equalsIgnoreCase(targetType);
    }

    /**
     * 判断是否为不影响数据检索和展示的ES属性
     */
    private boolean isNonEssentialESProperty(String propertyName) {
        return ES_SPECIFIC_FIELDS.contains(propertyName) ||
               propertyName.equals("creation_date") ||
               propertyName.equals("uuid") ||
               propertyName.equals("provided_name") ||
               propertyName.equals("version") ||
               propertyName.equals("_meta") ||
               propertyName.equals("_source") ||
               propertyName.equals("_all") ||
               propertyName.equals("_routing") ||
               propertyName.equals("_parent") ||
               propertyName.equals("_field_names");
    }

    /**
     * 判断是否为不影响数据检索和展示的POJO属性
     */
    private boolean isNonEssentialPOJOProperty(String propertyName) {
        return propertyName.equals("javaType") ||
               propertyName.equals("package") ||
               propertyName.equals("superclass") ||
               propertyName.equals("interfaces") ||
               propertyName.equals("annotations");
    }

    /**
     * 判断是否为可降低差异级别的属性
     */
    private boolean isReducibleProperty(String propertyName) {
        return propertyName.equals("comment") ||
               propertyName.equals("description") ||
               propertyName.equals("label") ||
               propertyName.equals("displayName") ||
               propertyName.equals("format") ||
               propertyName.equals("pattern");
    }

    /**
     * 比对列结构
     *
     * @param result 比对结果
     * @param config 比对配置
     */
    private void compareColumns(CompareResult result, DataSourceCompareConfig.CompareConfig config) {
        TableStructure sourceTable = result.getSourceTable();
        TableStructure targetTable = result.getTargetTable();

        // 获取当前表的配置
        DataSourceCompareConfig.TableCompareConfig tableConfig = config.getTableConfigs().get(0);
        List<String> ignoreFields = tableConfig.getIgnoreFields();

        // 创建映射，方便查找
        Map<String, ColumnStructure> sourceColumns = sourceTable.getColumns().stream()
                .collect(Collectors.toMap(ColumnStructure::getColumnName, c -> c, (c1, c2) -> c1));

        Map<String, ColumnStructure> targetColumns = targetTable.getColumns().stream()
                .collect(Collectors.toMap(ColumnStructure::getColumnName, c -> c, (c1, c2) -> c1));

        // 检查源表中存在但目标表不存在的列
        for (ColumnStructure sourceColumn : sourceTable.getColumns()) {
            String columnName = sourceColumn.getColumnName();

            // 跳过被忽略的字段
            if (ignoreFields != null && ignoreFields.contains(columnName)) {
                continue;
            }

            if (!targetColumns.containsKey(columnName)) {
                // 列缺失
                ColumnDifference diff = new ColumnDifference(
                        DifferenceType.COLUMN_MISSING,
                        DifferenceLevel.CRITICAL,
                        "Column exists in source but not in target",
                        columnName);
                diff.setSourceColumn(sourceColumn);

                result.getColumnDifferences().add(diff);
                result.incrementDifferenceCount(diff.getLevel());
            } else {
                // 列存在，需要比对细节
                ColumnStructure targetColumn = targetColumns.get(columnName);
                compareColumnDetails(result, config, sourceColumn, targetColumn);
            }
        }

        // 检查目标表中存在但源表不存在的列
        for (ColumnStructure targetColumn : targetTable.getColumns()) {
            String columnName = targetColumn.getColumnName();

            // 跳过被忽略的字段
            if (ignoreFields != null && ignoreFields.contains(columnName)) {
                continue;
            }

            if (!sourceColumns.containsKey(columnName)) {
                // 列缺失
                ColumnDifference diff = new ColumnDifference(
                        DifferenceType.COLUMN_MISSING,
                        DifferenceLevel.WARNING,
                        "Column exists in target but not in source",
                        columnName);
                diff.setTargetColumn(targetColumn);

                result.getColumnDifferences().add(diff);
                result.incrementDifferenceCount(diff.getLevel());
            }
        }
    }

    /**
     * 比对列结构细节
     */
    private void compareColumnDetails(CompareResult result, DataSourceCompareConfig.CompareConfig config,
            ColumnStructure sourceColumn, ColumnStructure targetColumn) {
        String columnName = sourceColumn.getColumnName();

        // Initial logging of received columns and their typeMappings (as added previously)
        if (logger.isDebugEnabled()) {
            logger.debug("Col '{}' (Source - {} - DataType: '{}') received in compareColumnDetails. TypeMappings:", 
                         columnName, result.getSourceTable().getSourceType(), sourceColumn.getDataType());
            if (sourceColumn.getTypeMappings() != null && !sourceColumn.getTypeMappings().isEmpty()) {
                for (ColumnStructure.TypeMapping tm : sourceColumn.getTypeMappings()) {
                    logger.debug("    - SourceCol '{}' Mapping: TargetDB='{}', MappedTypes={}", columnName, tm.getTargetType(), tm.getColumnTypes());
                }
            } else {
                logger.debug("    - SourceCol '{}': No TypeMappings present or list is empty.", columnName);
            }
            String targetColumnName = targetColumn.getColumnName() != null ? targetColumn.getColumnName() : "UNKNOWN_TARGET_COL_NAME";
            logger.debug("Col '{}' (Target - {} - DataType: '{}') received in compareColumnDetails. TypeMappings:", 
                         targetColumnName, result.getTargetTable().getSourceType(), targetColumn.getDataType());
            if (targetColumn.getTypeMappings() != null && !targetColumn.getTypeMappings().isEmpty()) {
                for (ColumnStructure.TypeMapping tm : targetColumn.getTypeMappings()) {
                    logger.debug("    - TargetCol '{}' Mapping: TargetDB='{}', MappedTypes={}", targetColumnName, tm.getTargetType(), tm.getColumnTypes());
                }
            } else {
                logger.debug("    - TargetCol '{}': No TypeMappings present or list is empty.", targetColumnName);
            }
        }

        DataSourceCompareConfig.TableCompareConfig tableConfig = config.getTableConfigs().get(0);
        List<String> ignoreFields = tableConfig.getIgnoreFields();

        if (ignoreFields != null && ignoreFields.contains(columnName)) {
            return;
        }

        boolean hasDifferences = false;
        ColumnDifference columnDiff = null; 

        String sourceDbType = result.getSourceTable().getSourceType();
        String targetDbType = result.getTargetTable().getSourceType();
        
        boolean sourceIsES = sourceDbType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);
        boolean targetIsES = targetDbType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);
        boolean sourceIsPOJO = sourceDbType.equalsIgnoreCase(DatabaseType.POJO);
        boolean targetIsPOJO = targetDbType.equalsIgnoreCase(DatabaseType.POJO);
        boolean isPojoOrEsComparison = sourceIsPOJO || targetIsPOJO || sourceIsES || targetIsES;

        String sDataTypeOriginal = sourceColumn.getDataType();
        String tDataTypeOriginal = targetColumn.getDataType();
        String sDataTypeLower = sDataTypeOriginal.toLowerCase();
        String tDataTypeLower = tDataTypeOriginal.toLowerCase();

        boolean typesCompatible = false;
        ColumnStructure.TypeMapping sourceToTargetMapping = sourceColumn.getTypeMapping(targetDbType);
        ColumnStructure.TypeMapping targetToSourceMapping = targetColumn.getTypeMapping(sourceDbType);

        logger.debug("Col '{}': Checking type compatibility. Source: '{}' (DB: {}), Target: '{}' (DB: {})", columnName, sDataTypeOriginal, sourceDbType, tDataTypeOriginal, targetDbType);
        if (sourceToTargetMapping != null) logger.debug("   SourceMapping to TargetDB ({} for col '{}'): {}", targetDbType, columnName, sourceToTargetMapping.getColumnTypes());
        if (targetToSourceMapping != null) logger.debug("   TargetMapping to SourceDB ({} for col '{}'): {}", sourceDbType, columnName, targetToSourceMapping.getColumnTypes());

        if (sourceIsPOJO) {
            if (targetIsES || isMySQLFamily(targetDbType)) { // POJO to ES or POJO to MySQL/TiDB
                logger.debug("Col '{}': Path A (POJO to DB/ES)", columnName);
                if (sourceToTargetMapping != null) {
                    typesCompatible = sourceToTargetMapping.hasColumnType(tDataTypeLower);
                    logger.debug("   Path A - Compatible by sourceToTargetMapping.hasColumnType(\"{}\"): {}", tDataTypeLower, typesCompatible);
                }
            } else { // POJO to POJO (or other non-ES/MySQL)
                 logger.debug("Col '{}': Path B (POJO to Other/POJO)", columnName);
                 typesCompatible = sDataTypeLower.equals(tDataTypeLower); 
                 logger.debug("   Path B - Compatible by direct string equality: {}", typesCompatible);
            }
        } else if (targetIsPOJO) {
            if (sourceIsES || isMySQLFamily(sourceDbType)) { // ES to POJO or MySQL/TiDB to POJO
                logger.debug("Col '{}': Path C (DB/ES to POJO)", columnName);
                if (targetToSourceMapping != null) { // target (POJO) column's mapping for sourceDB
                    typesCompatible = targetToSourceMapping.hasColumnType(sDataTypeLower);
                    logger.debug("   Path C - Compatible by targetToSourceMapping.hasColumnType(\"{}\"): {}", sDataTypeLower, typesCompatible);
                }
            } 
            // else: Other to POJO (will be covered by generic checks or direct equality if Path A didn't make it compatible)
        } 
        
        // Generic mapping checks if not yet compatible (e.g. ES to MySQL, or if POJO paths didn't fully resolve)
        if (!typesCompatible) {
            logger.debug("Col '{}': Path D (Generic mapping checks as not yet compatible or not primary POJO case)", columnName);
            if (sourceToTargetMapping != null && targetToSourceMapping != null) { 
                typesCompatible = sourceToTargetMapping.hasCommonColumnType(targetToSourceMapping);
                logger.debug("   Path D.1 - Compatible by hasCommonColumnType: {}", typesCompatible);
            }
            if (!typesCompatible && sourceToTargetMapping != null) {
                typesCompatible = sourceToTargetMapping.hasColumnType(tDataTypeLower);
                logger.debug("   Path D.2 - Compatible by sourceToTargetMapping.hasColumnType(\"{}\"): {}", tDataTypeLower, typesCompatible);
            }
            if (!typesCompatible && targetToSourceMapping != null) {
                typesCompatible = targetToSourceMapping.hasColumnType(sDataTypeLower);
                logger.debug("   Path D.3 - Compatible by targetToSourceMapping.hasColumnType(\"{}\"): {}", sDataTypeLower, typesCompatible);
            }
        }

        // Final fallback: direct string equality if still not compatible
        if (!typesCompatible) {
            logger.debug("Col '{}': Path E (Fallback to direct string equality)", columnName);
            typesCompatible = sDataTypeLower.equals(tDataTypeLower);
            logger.debug("   Path E - Compatible by sDataTypeLower.equals(tDataTypeLower): {}", typesCompatible);
        }
        
        logger.debug("Col '{}': Final typesCompatible = {}", columnName, typesCompatible);

        // EXPERIMENTAL CHANGE: If types are compatible and it's a POJO/ES comparison, consider it fully matched for this column and return early.
        if (typesCompatible && isPojoOrEsComparison) {
            logger.debug("Col '{}': Types compatible and POJO/ES comparison. Skipping further property checks and marking as no difference for this column.", columnName);
            // Ensure no ColumnDifference is created or added for this case if types are deemed compatible by mapping.
            // hasDifferences remains false by default.
            return; // Exit early, no differences to report for this column
        }

        if (!typesCompatible) {
            columnDiff = new ColumnDifference(DifferenceType.COLUMN_TYPE_DIFFERENT,
                    DifferenceLevel.CRITICAL, "Column type is different", columnName);
            columnDiff.setSourceColumn(sourceColumn);
            columnDiff.setTargetColumn(targetColumn);
            columnDiff.addPropertyDifference("dataType", sDataTypeOriginal, tDataTypeOriginal, DifferenceLevel.CRITICAL);
            hasDifferences = true;
            result.incrementDifferenceCount(DifferenceLevel.CRITICAL);
        } else {
            columnDiff = new ColumnDifference(DifferenceType.COLUMN_PROPERTY_DIFFERENT,
                    DifferenceLevel.NOTICE, "Column properties are different", columnName);
            columnDiff.setSourceColumn(sourceColumn);
            columnDiff.setTargetColumn(targetColumn);

            if (!isPojoOrEsComparison) { 
                if (sourceColumn.isNullable() != targetColumn.isNullable() && !isIgnoredType(config, "NULLABLE")) {
                    columnDiff.addPropertyDifference("nullable", sourceColumn.isNullable(), targetColumn.isNullable(), DifferenceLevel.WARNING);
                    hasDifferences = true;
                    result.incrementDifferenceCount(DifferenceLevel.WARNING);
                }
                if (!isDefaultValueEqual(sourceColumn.getDefaultValue(), targetColumn.getDefaultValue(),
                        sourceDbType, targetDbType) && !isIgnoredType(config, "DEFAULT")) {
                    columnDiff.addPropertyDifference("defaultValue", sourceColumn.getDefaultValue(), targetColumn.getDefaultValue(), DifferenceLevel.WARNING);
                    hasDifferences = true;
                    result.incrementDifferenceCount(DifferenceLevel.WARNING);
                }
                if (sourceColumn.isAutoIncrement() != targetColumn.isAutoIncrement()) {
                    boolean isSpecialCase = (isMySQLFamily(sourceDbType) && isMySQLFamily(targetDbType)) && 
                                            (sourceColumn.isAutoIncrement() && Boolean.TRUE.equals(targetColumn.getProperties().get("is_auto_random")));
                    if (!isSpecialCase && !isIgnoredType(config, "AUTO_INCREMENT")) {
                         columnDiff.addPropertyDifference("autoIncrement", sourceColumn.isAutoIncrement(), targetColumn.isAutoIncrement(), DifferenceLevel.WARNING);
                        hasDifferences = true;
                        result.incrementDifferenceCount(DifferenceLevel.WARNING);
                    }
                }
                boolean isInteger = isIntegerType(sDataTypeLower) || isIntegerType(tDataTypeLower);
                if (!isInteger) { 
                    compareColumnLengthProperties(columnDiff, sourceColumn, targetColumn, config, result, columnName);
                    if (columnDiff.getPropertyDifferences() != null && 
                        columnDiff.getPropertyDifferences().keySet().stream().anyMatch(key -> key.equals("length") || key.equals("precision") || key.equals("scale"))) {
                       hasDifferences = true; 
                    }
                }
                if (!isCommentEqual(sourceColumn.getComment(), targetColumn.getComment(), sourceDbType, targetDbType) && 
                    !isIgnoredType(config, "COMMENT")) {
                    // For non-POJO/ES, any comment difference is a difference
                    columnDiff.addPropertyDifference("comment", sourceColumn.getComment(), targetColumn.getComment(), DifferenceLevel.NOTICE);
                    hasDifferences = true;
                    result.incrementDifferenceCount(DifferenceLevel.NOTICE);
                }
            } 
        }

        if (hasDifferences && columnDiff != null) { 
            if ((columnDiff.getPropertyDifferences() != null && !columnDiff.getPropertyDifferences().isEmpty()) || 
                 columnDiff.getType() == DifferenceType.COLUMN_TYPE_DIFFERENT) {
                result.getColumnDifferences().add(columnDiff);
            }
        }
    }

    /**
     * 比较列的长度相关属性
     */
    private void compareColumnLengthProperties(ColumnDifference columnDiff, ColumnStructure sourceColumn,
            ColumnStructure targetColumn, DataSourceCompareConfig.CompareConfig config, CompareResult result, String columnName) {
        if (!Objects.equals(sourceColumn.getLength(), targetColumn.getLength()) &&
                !isIgnoredType(config, "LENGTH")) {
            columnDiff.addPropertyDifference("length",
                    sourceColumn.getLength(),
                    targetColumn.getLength(),
                    DifferenceLevel.WARNING);
            result.incrementDifferenceCount(DifferenceLevel.WARNING);
        }

        if (!Objects.equals(sourceColumn.getPrecision(), targetColumn.getPrecision()) &&
                !isIgnoredType(config, "PRECISION")) {
            columnDiff.addPropertyDifference("precision",
                    sourceColumn.getPrecision(),
                    targetColumn.getPrecision(),
                    DifferenceLevel.WARNING);
            result.incrementDifferenceCount(DifferenceLevel.WARNING);
        }

        if (!Objects.equals(sourceColumn.getScale(), targetColumn.getScale()) &&
                !isIgnoredType(config, "SCALE")) {
            columnDiff.addPropertyDifference("scale",
                    sourceColumn.getScale(),
                    targetColumn.getScale(),
                    DifferenceLevel.WARNING);
            result.incrementDifferenceCount(DifferenceLevel.WARNING);
        }
    }

    /**
     * 判断两个注释是否相等，处理MySQL家族数据库的特殊情况
     * 1. null和空字符串视为等价
     * 2. 去除首尾空格后比较
     * 3. 对于ES和MySQL的比对，忽略注释差异
     */
    private boolean isCommentEqual(String sourceComment, String targetComment, String sourceType, String targetType) {
        // 如果两个值都是null，则认为相等
        if (sourceComment == null && targetComment == null) {
            return true;
        }

        // 处理MySQL家族数据库的特殊情况
        String[] mysqlFamilyTypes = { DatabaseType.MYSQL, DatabaseType.TIDB };
        boolean isMySQLFamily = Arrays.stream(mysqlFamilyTypes).anyMatch(type -> type.equalsIgnoreCase(sourceType)) &&
                Arrays.stream(mysqlFamilyTypes).anyMatch(type -> type.equalsIgnoreCase(targetType));

        if (isMySQLFamily) {
            // 将null转换为空字符串
            String normalizedSource = sourceComment == null ? "" : sourceComment.trim();
            String normalizedTarget = targetComment == null ? "" : targetComment.trim();
            return normalizedSource.equals(normalizedTarget);
        }

        // 对于ES和MySQL的比对，忽略注释差异
        if ((sourceType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH) && isMySQLFamily(targetType)) ||
            (isMySQLFamily(sourceType) && targetType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH))) {
            return true;
        }

        // 其他情况直接比较原始值
        return Objects.equals(sourceComment, targetComment);
    }

    // 处理带引号的值，去掉外层引号后比较
    // 使用静态编译的正则表达式提升性能
    private static final Pattern QUOTE_PATTERN = Pattern.compile("^['\"].*['\"]$");
    private static final Pattern QUOTE_REPLACE_PATTERN = Pattern.compile("^['\"]|['\"]$");

    /**
     * 比较默认值是否相等，处理MySQL和ES的特殊情况
     *
     * @param sourceValue 源默认值
     * @param targetValue 目标默认值
     * @param sourceType  源数据库类型
     * @param targetType  目标数据库类型
     * @return 是否相等
     */
    private boolean isDefaultValueEqual(String sourceValue, String targetValue, String sourceType, String targetType) {
        // 如果两个值都是null，则认为相等
        if (sourceValue == null && targetValue == null) {
            return true;
        }

        boolean sourceIsES = sourceType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);
        boolean targetIsES = targetType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);

        // 如果一端是ES，且ES端没有默认值（null），则认为与MySQL端的默认值等价
        if ((sourceIsES && targetValue != null) || (targetIsES && sourceValue != null)) {
            return true;
        }

        // 标准化默认值
        String normalizedSource = normalizeDefaultValue(sourceValue, sourceType);
        String normalizedTarget = normalizeDefaultValue(targetValue, targetType);

        // 处理NULL值的情况
        if (isNullDefaultValue(normalizedSource, true) && isNullDefaultValue(normalizedTarget, true)) {
            return true;
        }

        // 处理CURRENT_TIMESTAMP的情况
        if (isCurrentTimestamp(normalizedSource) && isCurrentTimestamp(normalizedTarget)) {
            return true;
        }

        // 处理数值类型的情况
        if (isNumericDefaultValue(normalizedSource) && isNumericDefaultValue(normalizedTarget)) {
            return normalizeNumericValue(normalizedSource).equals(normalizeNumericValue(normalizedTarget));
        }

        // 其他情况直接比较标准化后的值
        return Objects.equals(normalizedSource, normalizedTarget);
    }

    /**
     * 标准化默认值
     */
    private String normalizeDefaultValue(String value, String dbType) {
        if (value == null) {
            return null;
        }

        String normalized = value.toLowerCase().trim();
        
        // 在normalizeDefaultValue方法内替换：
        // normalized = normalized.replaceAll("^default\\s+", "");
        normalized = DEFAULT_PREFIX_PATTERN.matcher(normalized).replaceFirst("");
        
        // 处理引号
        if (normalized.startsWith("'") && normalized.endsWith("'")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        if (normalized.startsWith("\"") && normalized.endsWith("\"")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }

        return normalized;
    }

    /**
     * 判断是否为NULL默认值
     */
    private boolean isNullDefaultValue(String value, boolean isNormalized) {
        if (value == null) {
            return true;
        }
        String normalized = isNormalized ? value : value.toLowerCase().trim();
        return normalized.isEmpty() ||
               normalized.equals("null") ||
               normalized.equals("'null'") ||
               normalized.equals("\"null\"") ||
               normalized.equals("default null") ||
               normalized.equals("'default null'") ||
               normalized.equals("\"default null\"") ||
               normalized.equals("default") ||
               normalized.equals("''") ||
               normalized.equals("\"\"");
    }

    /**
     * 判断是否为CURRENT_TIMESTAMP默认值
     */
    private boolean isCurrentTimestamp(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.toLowerCase().trim();
        return normalized.equals("current_timestamp") ||
               normalized.equals("current_timestamp()") ||
               normalized.matches("current_timestamp\\([0-9]*\\)");
    }

    /**
     * 判断是否为数值类型默认值
     */
    private boolean isNumericDefaultValue(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.toLowerCase().trim();
        return normalized.matches("^-?[0-9]+(\\.[0-9]+)?$") ||
               normalized.matches("^-?[0-9]+(\\.[0-9]+)?e-?[0-9]+$");
    }

    /**
     * 标准化数值类型默认值
     */
    private String normalizeNumericValue(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.toLowerCase().trim();
        // 移除前导零
        normalized = LEADING_ZERO_PATTERN.matcher(normalized).replaceAll("$1");
        // 移除小数点后的尾随零
        normalized = TRAILING_ZERO_PATTERN.matcher(normalized).replaceAll("");
        return normalized;
    }

    /**
     * 判断指定的差异类型是否被忽略
     *
     * @param config   比对配置
     * @param diffType 差异类型
     * @return 是否被忽略
     */
    private boolean isIgnoredType(DataSourceCompareConfig.CompareConfig config, String diffType) {
        if (config.getTableConfigs() == null || config.getTableConfigs().isEmpty()) {
            return false;
        }
        // 使用第一个表的配置进行判断
        List<String> ignoreTypes = config.getTableConfigs().get(0).getIgnoreTypes();
        return ignoreTypes != null && ignoreTypes.contains(diffType);
    }

    /**
     * 获取差异级别
     *
     * @param propertyName 属性名
     * @param config       比对配置
     * @return 差异级别
     */
    private DifferenceLevel getDifferenceLevel(String propertyName, DataSourceCompareConfig.CompareConfig config) {
        // 关键属性使用严重级别
        if (propertyName.contains("primary") || propertyName.contains("unique")) {
            return DifferenceLevel.CRITICAL;
        }

        // 性能相关属性使用警告级别
        if (propertyName.contains("index") || propertyName.contains("shard") ||
                propertyName.contains("replica") || propertyName.contains("partition")) {
            return DifferenceLevel.WARNING;
        }

        // 被忽略的类型使用可接受级别
        if (isIgnoredType(config, propertyName.toUpperCase(java.util.Locale.ROOT))) {
            return DifferenceLevel.ACCEPTABLE;
        }

        // 默认使用通知级别
        return DifferenceLevel.NOTICE;
    }

    /**
     * 比对索引结构
     *
     * @param result 比对结果
     * @param config 比对配置
     */
    private void compareIndexes(CompareResult result, DataSourceCompareConfig.CompareConfig config) {
        TableStructure sourceTable = result.getSourceTable();
        TableStructure targetTable = result.getTargetTable();
        
        String sourceType = sourceTable.getSourceType();
        String targetType = targetTable.getSourceType();
        
        boolean sourceIsES = sourceType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);
        boolean targetIsES = targetType.equalsIgnoreCase(DatabaseType.ELASTICSEARCH);
        boolean sourceIsPOJO = sourceType.equalsIgnoreCase(DatabaseType.POJO);
        boolean targetIsPOJO = targetType.equalsIgnoreCase(DatabaseType.POJO);

        // 如果源表或目标表是ES或POJO，则跳过索引比对
        if (sourceIsES || targetIsES || sourceIsPOJO || targetIsPOJO) {
            logger.debug("Skipping index comparison for {} vs {}", sourceType, targetType);
            return;
        }

        // 检查源表有但目标表没有的索引
        for (IndexStructure sourceIndex : sourceTable.getIndexes()) {
            boolean found = false;

            // 索引名称可能不同，需要基于列进行匹配
            for (IndexStructure targetIndex : targetTable.getIndexes()) {
                if (indexesEqual(sourceIndex, targetIndex)) {
                    found = true;

                    // 比对索引细节
                    compareIndexDetails(result, config, sourceIndex, targetIndex);
                    break;
                }
            }

            if (!found && !isIgnoredType(config, "INDEX")) {
                // 主键缺失是严重问题，普通索引缺失是警告
                DifferenceLevel level = sourceIndex.isPrimary() ? DifferenceLevel.CRITICAL : DifferenceLevel.WARNING;

                IndexDifference diff = new IndexDifference(
                        DifferenceType.INDEX_MISSING,
                        level,
                        "Index exists in source but not in target",
                        sourceIndex.getIndexName());
                diff.setSourceIndex(sourceIndex);

                result.getIndexDifferences().add(diff);
                result.incrementDifferenceCount(level);
            }
        }

        // 检查目标表有但源表没有的索引
        for (IndexStructure targetIndex : targetTable.getIndexes()) {
            boolean found = false;

            for (IndexStructure sourceIndex : sourceTable.getIndexes()) {
                if (indexesEqual(sourceIndex, targetIndex)) {
                    found = true;
                    break;
                }
            }

            if (!found && !isIgnoredType(config, "INDEX")) {
                // 目标表多的索引一般是优化目的，属于警告级别
                DifferenceLevel level = targetIndex.isPrimary() ? DifferenceLevel.CRITICAL : DifferenceLevel.NOTICE;

                IndexDifference diff = new IndexDifference(
                        DifferenceType.INDEX_MISSING,
                        level,
                        "Index exists in target but not in source",
                        targetIndex.getIndexName());
                diff.setTargetIndex(targetIndex);

                result.getIndexDifferences().add(diff);
                result.incrementDifferenceCount(level);
            }
        }
    }

    /**
     * 比对索引结构细节
     *
     * @param result      比对结果
     * @param config      比对配置
     * @param sourceIndex 源索引结构
     * @param targetIndex 目标索引结构
     */
    private void compareIndexDetails(CompareResult result, DataSourceCompareConfig.CompareConfig config,
            IndexStructure sourceIndex, IndexStructure targetIndex) {
        if (isIgnoredType(config, "INDEX_DETAIL")) {
            return;
        }

        boolean hasDifferences = false;

        IndexDifference indexDiff = new IndexDifference(
                DifferenceType.INDEX_STRUCTURE_DIFFERENT,
                DifferenceLevel.NOTICE,
                "Index properties are different",
                sourceIndex.getIndexName());
        indexDiff.setSourceIndex(sourceIndex);
        indexDiff.setTargetIndex(targetIndex);

        // 检查唯一性
        if (sourceIndex.isUnique() != targetIndex.isUnique() && !isIgnoredType(config, "UNIQUE")) {
            indexDiff.addPropertyDifference("unique",
                    sourceIndex.isUnique(),
                    targetIndex.isUnique(),
                    DifferenceLevel.WARNING);
            hasDifferences = true;
            result.incrementDifferenceCount(DifferenceLevel.WARNING);
        }

        // 检查索引类型
        if (!Objects.equals(sourceIndex.getIndexType(), targetIndex.getIndexType()) &&
                !isIgnoredType(config, "INDEX_TYPE")) {
            DifferenceLevel level = DifferenceLevel.NOTICE;
            // 如果主键类型不同，那么是严重问题
            if (sourceIndex.isPrimary() || targetIndex.isPrimary()) {
                level = DifferenceLevel.WARNING;
            }

            indexDiff.addPropertyDifference("indexType",
                    sourceIndex.getIndexType(),
                    targetIndex.getIndexType(),
                    level);
            hasDifferences = true;
            result.incrementDifferenceCount(level);
        }

        // 检查索引属性
        if (sourceIndex.getProperties() != null && targetIndex.getProperties() != null) {
            Set<String> allPropertyKeys = new HashSet<>();
            allPropertyKeys.addAll(sourceIndex.getProperties().keySet());
            allPropertyKeys.addAll(targetIndex.getProperties().keySet());

            for (String key : allPropertyKeys) {
                Object sourceValue = sourceIndex.getProperties().get(key);
                Object targetValue = targetIndex.getProperties().get(key);

                // 跳过一些非关键属性
                if (isNonEssentialIndexProperty(key)) {
                    continue;
                }

                if (!Objects.equals(sourceValue, targetValue)) {
                    DifferenceLevel level = getIndexPropertyDifferenceLevel(key);
                    indexDiff.addPropertyDifference(key,
                            sourceValue,
                            targetValue,
                            level);
                    hasDifferences = true;
                    result.incrementDifferenceCount(level);
                }
            }
        }

        if (hasDifferences) {
            result.getIndexDifferences().add(indexDiff);
        }
    }

    /**
     * 判断是否为非关键索引属性
     */
    private boolean isNonEssentialIndexProperty(String propertyName) {
        return propertyName.equals("comment") ||
               propertyName.equals("description") ||
               propertyName.equals("label") ||
               propertyName.equals("displayName") ||
               propertyName.equals("format") ||
               propertyName.equals("pattern");
    }

    /**
     * 获取索引属性差异级别
     */
    private DifferenceLevel getIndexPropertyDifferenceLevel(String propertyName) {
        // 关键属性使用警告级别
        if (propertyName.contains("unique") || 
            propertyName.contains("primary") || 
            propertyName.contains("type")) {
            return DifferenceLevel.WARNING;
        }

        // 性能相关属性使用通知级别
        if (propertyName.contains("sort") || 
            propertyName.contains("order") || 
            propertyName.contains("direction")) {
            return DifferenceLevel.NOTICE;
        }

        // 默认使用通知级别
        return DifferenceLevel.NOTICE;
    }

    /**
     * 判断两个索引是否相等（基于包含的列）
     *
     * @param index1 索引1
     * @param index2 索引2
     * @return 是否相等
     */
    private boolean indexesEqual(IndexStructure index1, IndexStructure index2) {
        // 主键索引有特殊匹配逻辑
        if (index1.isPrimary() && index2.isPrimary()) {
            return true;
        }

        // 根据索引列匹配
        if (index1.getColumns().size() != index2.getColumns().size()) {
            return false;
        }

        List<String> columns1 = index1.getColumns().stream()
                .map(IndexStructure.IndexColumnStructure::getColumnName)
                .collect(Collectors.toList());

        List<String> columns2 = index2.getColumns().stream()
                .map(IndexStructure.IndexColumnStructure::getColumnName)
                .collect(Collectors.toList());

        return columns1.containsAll(columns2) && columns2.containsAll(columns1);
    }

    /**
     * 计算整体匹配度
     *
     * @param result 比对结果
     */
    private void calculateMatchPercentage(CompareResult result) {
        TableStructure sourceTable = result.getSourceTable();
        TableStructure targetTable = result.getTargetTable();

        int totalColumnCount = Math.max(sourceTable.getColumns().size(), targetTable.getColumns().size());
        int totalIndexCount = Math.max(sourceTable.getIndexes().size(), targetTable.getIndexes().size());

        int columnDiffCount = result.getColumnDifferences().size();
        int indexDiffCount = result.getIndexDifferences().size();
        int tableDiffCount = result.getTableDifferences().size();

        double columnMatchRate = (totalColumnCount == 0) ? 100.0
                : (1.0 - (double) columnDiffCount / totalColumnCount) * 100.0;
        double indexMatchRate = (totalIndexCount == 0) ? 100.0
                : (1.0 - (double) indexDiffCount / totalIndexCount) * 100.0;

        // 表属性差异权重较小
        double tablePropertiesWeight = 0.1;
        double columnsWeight = 0.7;
        double indexesWeight = 0.2;

        double tablePropertiesMatchRate = (tableDiffCount == 0) ? 100.0
                : (1.0 - Math.min(tableDiffCount, 10) / 10.0) * 100.0;

        double matchPercentage = tablePropertiesMatchRate * tablePropertiesWeight +
                columnMatchRate * columnsWeight +
                indexMatchRate * indexesWeight;

        result.setMatchPercentage(Math.max(0.0, Math.min(100.0, matchPercentage)));
    }

    /**
     * 判断是否为MySQL家族数据库
     */
    private boolean isMySQLFamily(String sourceType) {
        return sourceType != null && (
            sourceType.equalsIgnoreCase(DatabaseType.MYSQL) ||
            sourceType.equalsIgnoreCase(DatabaseType.TIDB)
        );
    }

    /**
     * 判断是否为整数类型
     */
    private boolean isIntegerType(String type) {
        if (type == null) {
            return false;
        }
        String normalizedType = type.toLowerCase();
        return normalizedType.equals("int") ||
               normalizedType.equals("integer") ||
               normalizedType.equals("bigint") ||
               normalizedType.equals("long") ||
               normalizedType.equals("tinyint") ||
               normalizedType.equals("byte") ||
               normalizedType.equals("smallint") ||
               normalizedType.equals("short");
    }
}