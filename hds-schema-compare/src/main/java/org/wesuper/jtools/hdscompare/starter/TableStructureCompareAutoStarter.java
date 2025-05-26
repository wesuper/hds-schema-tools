package org.wesuper.jtools.hdscompare.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.wesuper.jtools.hdscompare.config.DataSourceConfig;
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
    private DataSourceConfig dataSourceConfig;
    
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
            logger.error("Error during automatic table structure comparison: {}", e.getMessage(), e);
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
        
        if (!result.getColumnDifferences().isEmpty()) {
            logger.info("Column differences ({}): ", result.getColumnDifferences().size());
            
            result.getColumnDifferences().forEach(diff -> {
                String level = "[" + diff.getLevel() + "]";
                switch (diff.getType()) {
                    case COLUMN_MISSING:
                        if (diff.getSourceColumn() != null) {
                            logger.info("  {} Column missing in target: {}", level, diff.getColumnName());
                        } else {
                            logger.info("  {} Column missing in source: {}", level, diff.getColumnName());
                        }
                        break;
                    case COLUMN_TYPE_DIFFERENT:
                    case COLUMN_PROPERTY_DIFFERENT:
                        if (verbose) {
                            logger.info("  {} Column '{}' differences:", level, diff.getColumnName());
                            diff.getPropertyDifferences().forEach((property, propDiff) -> {
                                logger.info("    - {}: {} → {}", property, propDiff.getSourceValue(), 
                                        propDiff.getTargetValue());
                            });
                        } else {
                            logger.info("  {} Column '{}' has different properties", level, diff.getColumnName());
                        }
                        break;
                    default:
                        logger.info("  {} Column '{}' has unknown difference type: {}", level, diff.getColumnName(), diff.getType());
                        break;
                }
            });
        }
        
        if (!result.getIndexDifferences().isEmpty()) {
            logger.info("Index differences ({}): ", result.getIndexDifferences().size());
            
            result.getIndexDifferences().forEach(diff -> {
                String level = "[" + diff.getLevel() + "]";
                switch (diff.getType()) {
                    case INDEX_MISSING:
                        if (diff.getSourceIndex() != null) {
                            logger.info("  {} Index missing in target: {}", level, diff.getIndexName());
                        } else {
                            logger.info("  {} Index missing in source: {}", level, diff.getIndexName());
                        }
                        break;
                    case INDEX_STRUCTURE_DIFFERENT:
                        if (verbose) {
                            logger.info("  {} Index '{}' differences:", level, diff.getIndexName());
                            diff.getPropertyDifferences().forEach((property, propDiff) -> {
                                logger.info("    - {}: {} → {}", property, propDiff.getSourceValue(), 
                                        propDiff.getTargetValue());
                            });
                        } else {
                            logger.info("  {} Index '{}' has different properties", level, diff.getIndexName());
                        }
                        break;
                    default:
                        logger.info("  {} Index '{}' has unknown difference type: {}", level, diff.getIndexName(), diff.getType());
                        break;
                }
            });
        }
        
        if (!result.getTableDifferences().isEmpty()) {
            logger.info("Table differences ({}): ", result.getTableDifferences().size());
            
            result.getTableDifferences().forEach(diff -> {
                String level = "[" + diff.getLevel() + "]";
                logger.info("  {} Table property '{}': {} → {}", level, diff.getPropertyName(), 
                        diff.getSourceValue(), diff.getTargetValue());
            });
        }
    }
} 