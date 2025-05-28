package org.wesuper.jtools.hdscompare.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.model.CompareResult;
import org.wesuper.jtools.hdscompare.service.TableStructureCompareService;

import java.util.List;

/**
 * 表结构比对自动启动器
 * 在Spring Boot应用启动时自动执行表结构比对
 *
 * @author vincentruan
 * @version 1.0.0
 */
public class TableStructureCompareAutoStarter implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(TableStructureCompareAutoStarter.class);
    
    @Autowired
    private DataSourceCompareConfig dataSourceConfig;
    
    @Autowired
    private TableStructureCompareService compareService;

    @Override
    public void run(ApplicationArguments args) {
        if (!dataSourceConfig.isAutoCompareOnStartup()) {
            logger.info("Table structure auto-compare is disabled");
            return;
        }
        
        logger.info("Starting automatic table structure comparison...");
        
        try {
            List<CompareResult> results = compareService.compareAllConfiguredTables();
            
            if (results.isEmpty()) {
                logger.warn("No table structure comparison results");
                return;
            }
            
            logComparisonResults(results);
        } catch (Exception e) {
            logger.error("Error during automatic table structure comparison", e);
        }
    }
    
    /**
     * 输出比对结果
     *
     * @param results 比对结果列表
     */
    private void logComparisonResults(List<CompareResult> results) {
        boolean hasWarnings = false;
        boolean hasCritical = false;
        
        for (CompareResult result : results) {
            if (result.isFullyMatched()) {
                logger.info("Table comparison '{}': MATCHED (100%)", result.getName());
            } else {
                logger.info("Table comparison '{}': DIFFERENCES FOUND ({}%)", 
                        result.getName(), String.format("%.2f", result.getMatchPercentage()));
                
                // 打印摘要
                printResultSummary(result);
                
                if (result.hasCriticalDifferences()) {
                    hasCritical = true;
                } else if (result.hasWarningDifferences()) {
                    hasWarnings = true;
                }
            }
        }
        
        // 输出总体结果
        if (hasCritical) {
            logger.error("TABLE STRUCTURE COMPARISON FOUND CRITICAL DIFFERENCES!");
        } else if (hasWarnings) {
            logger.warn("Table structure comparison found warnings");
        } else {
            logger.info("Table structure comparison completed successfully");
        }
    }
    
    /**
     * 打印结果摘要
     *
     * @param result 比对结果
     */
    private void printResultSummary(CompareResult result) {
        boolean verbose = dataSourceConfig.isVerboseOutput();
        StringBuilder summary = new StringBuilder();
        
        // 添加表比对名称和基本信息
        summary.append("\n=== Table Comparison: ").append(result.getName()).append(" ===\n");
        summary.append("Source Table: ").append(result.getSourceTable().getSourceType()).append(".").append(result.getSourceTable().getTableName()).append("\n");
        summary.append("Target Table: ").append(result.getTargetTable().getSourceType()).append(".").append(result.getTargetTable().getTableName()).append("\n");
        
        // 添加忽略的内容
        DataSourceCompareConfig.TableCompareConfig tableConfig = dataSourceConfig.getCompareConfigs().stream()
                .filter(config -> config.getName().equals(result.getName()))
                .findFirst()
                .map(config -> config.getTableConfigs().get(0))
                .orElse(null);
        if (tableConfig != null && tableConfig.getIgnoreFields() != null && !tableConfig.getIgnoreFields().isEmpty()) {
            summary.append("Ignored Fields: ").append(String.join(", ", tableConfig.getIgnoreFields())).append("\n");
        }
        if (tableConfig != null && tableConfig.getIgnoreTypes() != null && !tableConfig.getIgnoreTypes().isEmpty()) {
            summary.append("Ignored Types: ").append(String.join(", ", tableConfig.getIgnoreTypes())).append("\n");
        }
        
        // 添加列差异信息
        if (!result.getColumnDifferences().isEmpty()) {
            summary.append("\nColumn Differences (").append(result.getColumnDifferences().size()).append("):\n");
            result.getColumnDifferences().forEach(diff -> {
                String level = "[" + diff.getLevel() + "]";
                switch (diff.getType()) {
                    case COLUMN_MISSING:
                        if (diff.getSourceColumn() != null) {
                            summary.append("  ").append(level).append(" Column missing in target: ").append(diff.getColumnName()).append("\n");
                        } else {
                            summary.append("  ").append(level).append(" Column missing in source: ").append(diff.getColumnName()).append("\n");
                        }
                        break;
                    case COLUMN_TYPE_DIFFERENT:
                    case COLUMN_PROPERTY_DIFFERENT:
                        if (verbose) {
                            summary.append("  ").append(level).append(" Column '").append(diff.getColumnName()).append("' differences:\n");
                            diff.getPropertyDifferences().forEach((property, propDiff) -> {
                                summary.append("    - ").append(property).append(": ")
                                      .append(propDiff.getSourceValue()).append(" → ")
                                      .append(propDiff.getTargetValue()).append("\n");
                            });
                        } else {
                            summary.append("  ").append(level).append(" Column '").append(diff.getColumnName())
                                  .append("' has different properties\n");
                        }
                        break;
                    default:
                        summary.append("  ").append(level).append(" Column '").append(diff.getColumnName())
                              .append("' has unknown difference type: ").append(diff.getType()).append("\n");
                        break;
                }
            });
        }
        
        // 添加索引差异信息
        if (!result.getIndexDifferences().isEmpty()) {
            summary.append("\nIndex Differences (").append(result.getIndexDifferences().size()).append("):\n");
            result.getIndexDifferences().forEach(diff -> {
                String level = "[" + diff.getLevel() + "]";
                switch (diff.getType()) {
                    case INDEX_MISSING:
                        if (diff.getSourceIndex() != null) {
                            summary.append("  ").append(level).append(" Index missing in target: ").append(diff.getIndexName()).append("\n");
                        } else {
                            summary.append("  ").append(level).append(" Index missing in source: ").append(diff.getIndexName()).append("\n");
                        }
                        break;
                    case INDEX_STRUCTURE_DIFFERENT:
                        if (verbose) {
                            summary.append("  ").append(level).append(" Index '").append(diff.getIndexName()).append("' differences:\n");
                            diff.getPropertyDifferences().forEach((property, propDiff) -> {
                                summary.append("    - ").append(property).append(": ")
                                      .append(propDiff.getSourceValue()).append(" → ")
                                      .append(propDiff.getTargetValue()).append("\n");
                            });
                        } else {
                            summary.append("  ").append(level).append(" Index '").append(diff.getIndexName())
                                  .append("' has different properties\n");
                        }
                        break;
                    default:
                        summary.append("  ").append(level).append(" Index '").append(diff.getIndexName())
                              .append("' has unknown difference type: ").append(diff.getType()).append("\n");
                        break;
                }
            });
        }
        
        // 添加表属性差异信息
        if (!result.getTableDifferences().isEmpty()) {
            summary.append("\nTable Property Differences (").append(result.getTableDifferences().size()).append("):\n");
            result.getTableDifferences().forEach(diff -> {
                String level = "[" + diff.getLevel() + "]";
                summary.append("  ").append(level).append(" Property '").append(diff.getPropertyName())
                      .append("': ").append(diff.getSourceValue())
                      .append(" → ").append(diff.getTargetValue()).append("\n");
            });
        }
        
        // 添加比对结论
        summary.append("\nComparison Result: ");
        if (result.isFullyMatched()) {
            summary.append("FULLY MATCHED (100%)");
        } else {
            summary.append("DIFFERENCES FOUND (").append(String.format("%.2f", result.getMatchPercentage())).append("%)");
            if (result.hasCriticalDifferences()) {
                summary.append(" [CRITICAL DIFFERENCES]");
            } else if (result.hasWarningDifferences()) {
                summary.append(" [WARNINGS]");
            }
        }
        
        // 一次性输出所有信息
        logger.info(summary.toString());
    }
} 