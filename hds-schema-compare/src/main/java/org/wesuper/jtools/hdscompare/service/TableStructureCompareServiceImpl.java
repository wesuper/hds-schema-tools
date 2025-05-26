package org.wesuper.jtools.hdscompare.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.wesuper.jtools.hdscompare.config.DataSourceConfig;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
    private DataSourceConfig dataSourceConfig;
    
    @Autowired
    private TableStructureExtractorFactory extractorFactory;
    
    @Override
    public CompareResult compareTableStructures(TableStructure sourceTable, TableStructure targetTable, 
                                              DataSourceConfig.CompareConfig config) {
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
        
        List<DataSourceConfig.CompareConfig> configs = dataSourceConfig.getCompareConfigs();
        if (configs == null || configs.isEmpty()) {
            logger.warn("No table compare configurations found");
            return results;
        }
        
        for (DataSourceConfig.CompareConfig config : configs) {
            try {
                CompareResult result = compareTablesByConfig(config);
                if (result != null) {
                    results.add(result);
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
        
        DataSourceConfig.CompareConfig config = dataSourceConfig.getCompareConfigs().stream()
                .filter(c -> name.equals(c.getName()))
                .findFirst()
                .orElse(null);
        
        if (config == null) {
            logger.warn("No compare configuration found with name: {}", name);
            return null;
        }
        
        return compareTablesByConfig(config);
    }
    
    @Override
    public TableStructure getTableStructure(DataSourceConfig.TableConfig tableConfig) throws Exception {
        if (tableConfig == null) {
            throw new IllegalArgumentException("Table config cannot be null");
        }
        
        String sourceType = tableConfig.getType();
        if (!StringUtils.hasText(sourceType)) {
            throw new IllegalArgumentException("Source type cannot be empty");
        }
        
        TableStructureExtractor extractor = extractorFactory.getExtractor(sourceType);
        if (extractor == null) {
            throw new IllegalArgumentException("No extractor found for source type: " + sourceType);
        }
        
        return extractor.extractTableStructure(tableConfig);
    }
    
    /**
     * 根据比对配置比对表结构
     *
     * @param config 比对配置
     * @return 比对结果
     */
    private CompareResult compareTablesByConfig(DataSourceConfig.CompareConfig config) {
        try {
            // 获取源表结构
            TableStructure sourceTable = getTableStructure(config.getSourceTable());
            
            // 获取目标表结构
            TableStructure targetTable = getTableStructure(config.getTargetTable());
            
            // 比对结构
            return compareTableStructures(sourceTable, targetTable, config);
        } catch (Exception e) {
            logger.error("Failed to compare tables with config {}: {}", config.getName(), e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 比对表级属性
     *
     * @param result 比对结果
     * @param config 比对配置
     */
    private void compareTableProperties(CompareResult result, DataSourceConfig.CompareConfig config) {
        TableStructure sourceTable = result.getSourceTable();
        TableStructure targetTable = result.getTargetTable();
        
        // 检查表注释
        if (!Objects.equals(sourceTable.getTableComment(), targetTable.getTableComment()) &&
                !isIgnoredType(config, "COMMENT")) {
            TableDifference diff = new TableDifference(
                    DifferenceType.TABLE_PROPERTY_DIFFERENT,
                    DifferenceLevel.NOTICE,
                    "Table comment is different",
                    "comment",
                    sourceTable.getTableComment(),
                    targetTable.getTableComment()
            );
            
            result.getTableDifferences().add(diff);
            result.incrementDifferenceCount(diff.getLevel());
        }
        
        // 比对表的特殊属性
        if (sourceTable.getProperties() != null && targetTable.getProperties() != null) {
            for (Map.Entry<String, Object> entry : sourceTable.getProperties().entrySet()) {
                String key = entry.getKey();
                Object sourceValue = entry.getValue();
                Object targetValue = targetTable.getProperties().get(key);
                
                if (!Objects.equals(sourceValue, targetValue)) {
                    DifferenceLevel level = getDifferenceLevel(key, config);
                    TableDifference diff = new TableDifference(
                            DifferenceType.TABLE_PROPERTY_DIFFERENT,
                            level,
                            "Table property '" + key + "' is different",
                            key,
                            sourceValue,
                            targetValue
                    );
                    
                    result.getTableDifferences().add(diff);
                    result.incrementDifferenceCount(level);
                }
            }
        }
    }
    
    /**
     * 比对列结构
     *
     * @param result 比对结果
     * @param config 比对配置
     */
    private void compareColumns(CompareResult result, DataSourceConfig.CompareConfig config) {
        TableStructure sourceTable = result.getSourceTable();
        TableStructure targetTable = result.getTargetTable();
        
        List<String> ignoreFields = config.getIgnoreFields();
        
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
                        columnName
                );
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
                        columnName
                );
                diff.setTargetColumn(targetColumn);
                
                result.getColumnDifferences().add(diff);
                result.incrementDifferenceCount(diff.getLevel());
            }
        }
    }
    
    /**
     * 比对列结构细节
     *
     * @param result 比对结果
     * @param config 比对配置
     * @param sourceColumn 源列结构
     * @param targetColumn 目标列结构
     */
    private void compareColumnDetails(CompareResult result, DataSourceConfig.CompareConfig config, 
                                    ColumnStructure sourceColumn, ColumnStructure targetColumn) {
        String columnName = sourceColumn.getColumnName();
        
        // 如果是忽略的字段，直接返回
        List<String> ignoreFields = config.getIgnoreFields();
        if (ignoreFields != null && ignoreFields.contains(columnName)) {
            return;
        }
        
        boolean hasDifferences = false;
        
        ColumnDifference columnDiff = null;
        
        // 检查数据类型
        if (!Objects.equals(sourceColumn.getDataType(), targetColumn.getDataType()) ||
                !Objects.equals(sourceColumn.getColumnType(), targetColumn.getColumnType())) {
            
            columnDiff = new ColumnDifference(
                    DifferenceType.COLUMN_TYPE_DIFFERENT, 
                    DifferenceLevel.CRITICAL,
                    "Column type is different", 
                    columnName
            );
            columnDiff.setSourceColumn(sourceColumn);
            columnDiff.setTargetColumn(targetColumn);
            
            // 添加属性差异
            columnDiff.addPropertyDifference("dataType", 
                    sourceColumn.getDataType(), 
                    targetColumn.getDataType(), 
                    DifferenceLevel.CRITICAL);
                    
            columnDiff.addPropertyDifference("columnType", 
                    sourceColumn.getColumnType(), 
                    targetColumn.getColumnType(), 
                    DifferenceLevel.CRITICAL);
            
            hasDifferences = true;
            result.incrementDifferenceCount(DifferenceLevel.CRITICAL);
        } else {
            // 数据类型相同，检查其他属性
            columnDiff = new ColumnDifference(
                    DifferenceType.COLUMN_PROPERTY_DIFFERENT,
                    DifferenceLevel.NOTICE,
                    "Column properties are different",
                    columnName
            );
            columnDiff.setSourceColumn(sourceColumn);
            columnDiff.setTargetColumn(targetColumn);
            
            // 检查是否允许为空
            if (sourceColumn.isNullable() != targetColumn.isNullable() &&
                    !isIgnoredType(config, "NULLABLE")) {
                columnDiff.addPropertyDifference("nullable", 
                        sourceColumn.isNullable(), 
                        targetColumn.isNullable(), 
                        DifferenceLevel.WARNING);
                hasDifferences = true;
                result.incrementDifferenceCount(DifferenceLevel.WARNING);
            }
            
            // 检查默认值
            if (!Objects.equals(sourceColumn.getDefaultValue(), targetColumn.getDefaultValue()) &&
                    !isIgnoredType(config, "DEFAULT")) {
                columnDiff.addPropertyDifference("defaultValue", 
                        sourceColumn.getDefaultValue(), 
                        targetColumn.getDefaultValue(), 
                        DifferenceLevel.WARNING);
                hasDifferences = true;
                result.incrementDifferenceCount(DifferenceLevel.WARNING);
            }
            
            // 检查长度/精度/小数位数
            if (!Objects.equals(sourceColumn.getLength(), targetColumn.getLength()) &&
                    !isIgnoredType(config, "LENGTH")) {
                columnDiff.addPropertyDifference("length", 
                        sourceColumn.getLength(), 
                        targetColumn.getLength(), 
                        DifferenceLevel.WARNING);
                hasDifferences = true;
                result.incrementDifferenceCount(DifferenceLevel.WARNING);
            }
            
            if (!Objects.equals(sourceColumn.getPrecision(), targetColumn.getPrecision()) &&
                    !isIgnoredType(config, "PRECISION")) {
                columnDiff.addPropertyDifference("precision", 
                        sourceColumn.getPrecision(), 
                        targetColumn.getPrecision(), 
                        DifferenceLevel.WARNING);
                hasDifferences = true;
                result.incrementDifferenceCount(DifferenceLevel.WARNING);
            }
            
            if (!Objects.equals(sourceColumn.getScale(), targetColumn.getScale()) &&
                    !isIgnoredType(config, "SCALE")) {
                columnDiff.addPropertyDifference("scale", 
                        sourceColumn.getScale(), 
                        targetColumn.getScale(), 
                        DifferenceLevel.WARNING);
                hasDifferences = true;
                result.incrementDifferenceCount(DifferenceLevel.WARNING);
            }
            
            // 检查自增属性
            if (sourceColumn.isAutoIncrement() != targetColumn.isAutoIncrement()) {
                // 特殊处理：MySQL的AUTO_INCREMENT和TiDB的AUTO_RANDOM是相似的功能
                boolean isSpecialCase = 
                        (result.getSourceTable().getSourceType().equalsIgnoreCase("mysql") && 
                         result.getTargetTable().getSourceType().equalsIgnoreCase("tidb")) &&
                        (sourceColumn.isAutoIncrement() && 
                         Boolean.TRUE.equals(targetColumn.getProperties().get("is_auto_random")));
                
                if (!isSpecialCase && !isIgnoredType(config, "AUTO_INCREMENT")) {
                    columnDiff.addPropertyDifference("autoIncrement", 
                            sourceColumn.isAutoIncrement(), 
                            targetColumn.isAutoIncrement(), 
                            DifferenceLevel.WARNING);
                    hasDifferences = true;
                    result.incrementDifferenceCount(DifferenceLevel.WARNING);
                }
            }
            
            // 检查注释
            if (!Objects.equals(sourceColumn.getComment(), targetColumn.getComment()) &&
                    !isIgnoredType(config, "COMMENT")) {
                columnDiff.addPropertyDifference("comment", 
                        sourceColumn.getComment(), 
                        targetColumn.getComment(), 
                        DifferenceLevel.NOTICE);
                hasDifferences = true;
                result.incrementDifferenceCount(DifferenceLevel.NOTICE);
            }
        }
        
        if (hasDifferences) {
            result.getColumnDifferences().add(columnDiff);
        }
    }
    
    /**
     * 判断指定的差异类型是否被忽略
     *
     * @param config 比对配置
     * @param diffType 差异类型
     * @return 是否被忽略
     */
    private boolean isIgnoredType(DataSourceConfig.CompareConfig config, String diffType) {
        List<String> ignoreTypes = config.getIgnoreTypes();
        return ignoreTypes != null && ignoreTypes.contains(diffType);
    }
    
    /**
     * 获取差异级别
     *
     * @param propertyName 属性名
     * @param config 比对配置
     * @return 差异级别
     */
    private DifferenceLevel getDifferenceLevel(String propertyName, DataSourceConfig.CompareConfig config) {
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
    private void compareIndexes(CompareResult result, DataSourceConfig.CompareConfig config) {
        TableStructure sourceTable = result.getSourceTable();
        TableStructure targetTable = result.getTargetTable();
        
        // 检查源表有但目标表没有的索引
        for (IndexStructure sourceIndex : sourceTable.getIndexes()) {
            // 在Elasticsearch中，每个字段默认都有索引，不需要额外比对，除非配置为显式比对ES索引
            if (targetTable.getSourceType().equalsIgnoreCase("elasticsearch")) {
                Object compareEsIndexes = config.getSourceTable().getProperties().get("compare_es_indexes");
                if (!Boolean.TRUE.equals(compareEsIndexes)) {
                    continue;
                }
            }
            
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
                DifferenceLevel level = sourceIndex.isPrimary() ? 
                        DifferenceLevel.CRITICAL : DifferenceLevel.WARNING;
                
                IndexDifference diff = new IndexDifference(
                        DifferenceType.INDEX_MISSING,
                        level,
                        "Index exists in source but not in target",
                        sourceIndex.getIndexName()
                );
                diff.setSourceIndex(sourceIndex);
                
                result.getIndexDifferences().add(diff);
                result.incrementDifferenceCount(level);
            }
        }
        
        // 检查目标表有但源表没有的索引
        for (IndexStructure targetIndex : targetTable.getIndexes()) {
            // 在Elasticsearch中，每个字段默认都有索引，不需要额外比对，除非配置为显式比对ES索引
            if (sourceTable.getSourceType().equalsIgnoreCase("elasticsearch")) {
                Object compareEsIndexes = config.getTargetTable().getProperties().get("compare_es_indexes");
                if (!Boolean.TRUE.equals(compareEsIndexes)) {
                    continue;
                }
            }
            
            boolean found = false;
            
            for (IndexStructure sourceIndex : sourceTable.getIndexes()) {
                if (indexesEqual(sourceIndex, targetIndex)) {
                    found = true;
                    break;
                }
            }
            
            if (!found && !isIgnoredType(config, "INDEX")) {
                // 目标表多的索引一般是优化目的，属于警告级别
                DifferenceLevel level = targetIndex.isPrimary() ? 
                        DifferenceLevel.CRITICAL : DifferenceLevel.NOTICE;
                
                IndexDifference diff = new IndexDifference(
                        DifferenceType.INDEX_MISSING,
                        level,
                        "Index exists in target but not in source",
                        targetIndex.getIndexName()
                );
                diff.setTargetIndex(targetIndex);
                
                result.getIndexDifferences().add(diff);
                result.incrementDifferenceCount(level);
            }
        }
    }
    
    /**
     * 比对索引结构细节
     *
     * @param result 比对结果
     * @param config 比对配置
     * @param sourceIndex 源索引结构
     * @param targetIndex 目标索引结构
     */
    private void compareIndexDetails(CompareResult result, DataSourceConfig.CompareConfig config, 
                                  IndexStructure sourceIndex, IndexStructure targetIndex) {
        if (isIgnoredType(config, "INDEX_DETAIL")) {
            return;
        }
        
        boolean hasDifferences = false;
        
        IndexDifference indexDiff = new IndexDifference(
                DifferenceType.INDEX_STRUCTURE_DIFFERENT,
                DifferenceLevel.NOTICE,
                "Index properties are different",
                sourceIndex.getIndexName()
        );
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
        
        if (hasDifferences) {
            result.getIndexDifferences().add(indexDiff);
        }
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
        
        double columnMatchRate = (totalColumnCount == 0) ? 100.0 : 
                (1.0 - (double) columnDiffCount / totalColumnCount) * 100.0;
        double indexMatchRate = (totalIndexCount == 0) ? 100.0 : 
                (1.0 - (double) indexDiffCount / totalIndexCount) * 100.0;
        
        // 表属性差异权重较小
        double tablePropertiesWeight = 0.1;
        double columnsWeight = 0.7;
        double indexesWeight = 0.2;
        
        double tablePropertiesMatchRate = (tableDiffCount == 0) ? 100.0 : 
                (1.0 - Math.min(tableDiffCount, 10) / 10.0) * 100.0;
        
        double matchPercentage = 
                tablePropertiesMatchRate * tablePropertiesWeight +
                columnMatchRate * columnsWeight +
                indexMatchRate * indexesWeight;
        
        result.setMatchPercentage(Math.max(0.0, Math.min(100.0, matchPercentage)));
    }
} 