package org.wesuper.jtools.hdscompare.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.model.CompareResult;
import org.wesuper.jtools.hdscompare.service.TableStructureCompareService;
import org.wesuper.jtools.hdscompare.model.CompareResult.DifferenceLevel;
import org.wesuper.jtools.hdscompare.model.CompareResult.TableDifference;
import org.wesuper.jtools.hdscompare.model.CompareResult.ColumnDifference;
import org.wesuper.jtools.hdscompare.model.CompareResult.IndexDifference;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

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

            if (dataSourceConfig.isEnableMarkdownOutput()) {
                writeResultsToMarkdownFile(results, dataSourceConfig.getMarkdownOutputFilePath());
            }

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

    private void writeResultsToMarkdownFile(List<CompareResult> results, String filePath) {
        logger.info("Writing comparison results to Markdown file: {}", filePath);
        StringBuilder markdownBuilder = new StringBuilder();
        markdownBuilder.append("# Table Structure Comparison Report\n\n");

        for (CompareResult result : results) {
            DataSourceCompareConfig.TableCompareConfig tableConfig = dataSourceConfig.getCompareConfigs().stream()
                    .filter(config -> config.getName().equals(result.getName()))
                    .findFirst()
                    .flatMap(config -> config.getTableConfigs().stream().findFirst())
                    .orElse(null);
            markdownBuilder.append(formatResultToMarkdown(result, tableConfig));
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(markdownBuilder.toString());
            logger.info("Successfully wrote comparison results to {}", filePath);
        } catch (IOException e) {
            logger.error("Failed to write Markdown report to {}: {}", filePath, e.getMessage(), e);
        }
    }

    private String formatResultToMarkdown(CompareResult result, DataSourceCompareConfig.TableCompareConfig tableConfig) {
        StringBuilder md = new StringBuilder();

        md.append("## Comparison: ").append(result.getName()).append("\n\n");

        if (result.isFullyMatched()) {
            md.append("**Result: ✅ FULLY MATCHED** (100%)\n\n");
        } else {
            md.append("**Result: ❌ DIFFERENCES FOUND** (").append(String.format("%.2f", result.getMatchPercentage())).append("%)\n");
            if (result.hasCriticalDifferences()) {
                md.append("  ❗ **Contains CRITICAL Differences**\n");
            } else if (result.hasWarningDifferences()) {
                md.append("  ⚠️ **Contains WARNINGS**\n");
            }
            md.append("\n");
        }

        md.append("### Overview\n");
        md.append("- **Source Table:** `").append(result.getSourceTable().getSourceType()).append("`.`").append(result.getSourceTable().getTableName()).append("`\n");
        md.append("- **Target Table:** `").append(result.getTargetTable().getSourceType()).append("`.`").append(result.getTargetTable().getTableName()).append("`\n");

        if (tableConfig != null) {
            if (tableConfig.getIgnoreFields() != null && !tableConfig.getIgnoreFields().isEmpty()) {
                md.append("- **Ignored Fields:** `").append(String.join("`, `", tableConfig.getIgnoreFields())).append("`\n");
            }
            if (tableConfig.getIgnoreTypes() != null && !tableConfig.getIgnoreTypes().isEmpty()) {
                md.append("- **Ignored Types:** `").append(String.join("`, `", tableConfig.getIgnoreTypes())).append("`\n");
            }
        }
        md.append("\n");

        if (!result.getTableDifferences().isEmpty()) {
            md.append("### Ⅲ Table Property Differences\n");
            md.append("| Property | Level | Source Value | Target Value |\n");
            md.append("|----------|-------|--------------|--------------|\n");
            for (TableDifference diff : result.getTableDifferences()) {
                md.append("| `").append(diff.getPropertyName()).append("` ");
                md.append("| ").append(formatDifferenceLevel(diff.getLevel())).append(" ");
                md.append("| `").append(escapeMarkdown(String.valueOf(diff.getSourceValue()))).append("` ");
                md.append("| `").append(escapeMarkdown(String.valueOf(diff.getTargetValue()))).append("` |\n");
            }
            md.append("\n");
        }

        if (!result.getColumnDifferences().isEmpty()) {
            md.append("### Ⅱ Column Differences\n");
            md.append("| Column Name | Type | Level | Details |\n");
            md.append("|-------------|------|-------|---------|\n");
            for (ColumnDifference diff : result.getColumnDifferences()) {
                md.append("| `").append(diff.getColumnName()).append("` ");
                md.append("| ").append(diff.getType()).append(" ");
                md.append("| ").append(formatDifferenceLevel(diff.getLevel())).append(" ");
                if (diff.getType() == CompareResult.DifferenceType.COLUMN_MISSING) {
                    md.append("| Missing in ").append(diff.getSourceColumn() != null ? "target" : "source").append(" |\n");
                } else {
                    String details = diff.getPropertyDifferences().entrySet().stream()
                        .map(entry -> String.format("`%s`: `%s` → `%s` (%s)", 
                                     escapeMarkdown(entry.getKey()), 
                                     escapeMarkdown(String.valueOf(entry.getValue().getSourceValue())), 
                                     escapeMarkdown(String.valueOf(entry.getValue().getTargetValue())),
                                     formatDifferenceLevel(entry.getValue().getLevel())))
                        .collect(Collectors.joining("<br>"));
                    md.append("| ").append(details).append(" |\n");
                }
            }
            md.append("\n");
        }

        if (!result.getIndexDifferences().isEmpty()) {
            md.append("### I Index Differences\n");
            md.append("| Index Name | Type | Level | Details |\n");
            md.append("|------------|------|-------|---------|\n");
            for (IndexDifference diff : result.getIndexDifferences()) {
                md.append("| `").append(diff.getIndexName()).append("` ");
                md.append("| ").append(diff.getType()).append(" ");
                md.append("| ").append(formatDifferenceLevel(diff.getLevel())).append(" ");
                 if (diff.getType() == CompareResult.DifferenceType.INDEX_MISSING) {
                    md.append("| Missing in ").append(diff.getSourceIndex() != null ? "target" : "source").append(" |\n");
                } else {
                     String details = diff.getPropertyDifferences().entrySet().stream()
                        .map(entry -> String.format("`%s`: `%s` → `%s` (%s)",
                                     escapeMarkdown(entry.getKey()), 
                                     escapeMarkdown(String.valueOf(entry.getValue().getSourceValue())), 
                                     escapeMarkdown(String.valueOf(entry.getValue().getTargetValue())),
                                     formatDifferenceLevel(entry.getValue().getLevel())))
                        .collect(Collectors.joining("<br>"));
                    md.append("| ").append(details).append(" |\n");
                }
            }
            md.append("\n");
        }
        md.append("---\n\n"); // Separator for multiple results
        return md.toString();
    }

    private String formatDifferenceLevel(DifferenceLevel level) {
        switch (level) {
            case CRITICAL:
                return "❗ CRITICAL";
            case WARNING:
                return "⚠️ WARNING";
            case NOTICE:
                return "ℹ️ NOTICE";
            case ACCEPTABLE:
                return "✅ ACCEPTABLE";
            default:
                return level.toString();
        }
    }

    private String escapeMarkdown(String text) {
        if (text == null) return "null";
        return text.replace("|", "\\\\|").replace("`", "\\\\`").replace("*", "\\\\*").replace("_", "\\\\_").replace("[", "\\\\[").replace("]", "\\\\]");
    }
} 