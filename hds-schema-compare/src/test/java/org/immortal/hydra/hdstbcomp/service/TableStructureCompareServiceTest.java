package org.immortal.hydra.hdstbcomp.service;

import org.immortal.hydra.hdstbcomp.config.DataSourceConfig;
import org.immortal.hydra.hdstbcomp.extractor.MySqlTableStructureExtractor;
import org.immortal.hydra.hdstbcomp.extractor.TableStructureExtractor;
import org.immortal.hydra.hdstbcomp.extractor.TableStructureExtractorFactory;
import org.immortal.hydra.hdstbcomp.model.ColumnStructure;
import org.immortal.hydra.hdstbcomp.model.CompareResult;
import org.immortal.hydra.hdstbcomp.model.TableStructure;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * 表结构比对服务单元测试
 */
@ExtendWith(MockitoExtension.class)
public class TableStructureCompareServiceTest {

    @InjectMocks
    private TableStructureCompareServiceImpl compareService;

    @Mock
    private TableStructureExtractorFactory extractorFactory;

    @Mock
    private DataSourceConfig dataSourceConfig;

    private EmbeddedDatabase h2Database;
    private Map<String, DataSource> dataSourceMap;
    private TableStructureExtractor mysqlExtractor;
    private TableStructureExtractor tidbExtractor;

    @BeforeEach
    public void setup() {
        // 创建H2嵌入式数据库
        h2Database = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:schema.sql")
                .build();

        // 创建数据源映射
        dataSourceMap = new HashMap<>();
        dataSourceMap.put("mysqlDataSource", h2Database);
        dataSourceMap.put("tidbDataSource", h2Database);

        // 创建并配置MySQL提取器
        mysqlExtractor = new MySqlTableStructureExtractor();
        setFieldByReflection(mysqlExtractor, "dataSourceMap", dataSourceMap);
        
        // 由于TiDB提取器继承自MySQL提取器，这里可以直接使用MySQL提取器
        tidbExtractor = new MySqlTableStructureExtractor();
        setFieldByReflection(tidbExtractor, "dataSourceMap", dataSourceMap);

        // 配置提取器工厂
        when(extractorFactory.getExtractor("mysql")).thenReturn(mysqlExtractor);
        when(extractorFactory.getExtractor("tidb")).thenReturn(tidbExtractor);
    }

    @Test
    public void testCompareTableStructures_WithIgnoredFields() throws Exception {
        // 准备测试数据 - 源表配置
        DataSourceConfig.TableConfig sourceTableConfig = new DataSourceConfig.TableConfig();
        sourceTableConfig.setType("mysql");
        sourceTableConfig.setDataSourceName("mysqlDataSource");
        sourceTableConfig.setTableName("mysql_table");

        // 准备测试数据 - 目标表配置
        DataSourceConfig.TableConfig targetTableConfig = new DataSourceConfig.TableConfig();
        targetTableConfig.setType("tidb");
        targetTableConfig.setDataSourceName("tidbDataSource");
        targetTableConfig.setTableName("tidb_table");

        // 获取表结构
        TableStructure sourceTable = compareService.getTableStructure(sourceTableConfig);
        TableStructure targetTable = compareService.getTableStructure(targetTableConfig);

        // 设置比对配置
        DataSourceConfig.CompareConfig compareConfig = new DataSourceConfig.CompareConfig();
        compareConfig.setName("test-compare");
        compareConfig.setIgnoreFields(Arrays.asList("create_time", "update_time"));
        compareConfig.setIgnoreTypes(Arrays.asList("INDEX", "COMMENT"));

        // 执行比对
        CompareResult result = compareService.compareTableStructures(sourceTable, targetTable, compareConfig);

        // 验证结果
        assertNotNull(result);
        assertTrue(result.getColumnDifferences().size() > 0, "比对结果应该包含列差异");
        
        // 验证被忽略的字段没有出现在差异列表中
        result.getColumnDifferences().forEach(diff -> {
            assertFalse("create_time".equals(diff.getColumnName()) || "update_time".equals(diff.getColumnName()), 
                "被忽略的字段不应该出现在差异中");
        });

        // 验证索引差异被忽略
        assertEquals(0, result.getIndexDifferences().size(), "索引差异应该被忽略");
    }
    
    @Test
    public void testCompareTableStructures_WithNoIgnoredFields() throws Exception {
        // 准备测试数据 - 源表配置
        DataSourceConfig.TableConfig sourceTableConfig = new DataSourceConfig.TableConfig();
        sourceTableConfig.setType("mysql");
        sourceTableConfig.setDataSourceName("mysqlDataSource");
        sourceTableConfig.setTableName("mysql_table");

        // 准备测试数据 - 目标表配置
        DataSourceConfig.TableConfig targetTableConfig = new DataSourceConfig.TableConfig();
        targetTableConfig.setType("tidb");
        targetTableConfig.setDataSourceName("tidbDataSource");
        targetTableConfig.setTableName("tidb_table");

        // 获取表结构
        TableStructure sourceTable = compareService.getTableStructure(sourceTableConfig);
        TableStructure targetTable = compareService.getTableStructure(targetTableConfig);

        // 设置比对配置 - 不忽略任何字段和类型
        DataSourceConfig.CompareConfig compareConfig = new DataSourceConfig.CompareConfig();
        compareConfig.setName("test-compare-no-ignore");
        compareConfig.setIgnoreFields(new ArrayList<>());
        compareConfig.setIgnoreTypes(new ArrayList<>());

        // 执行比对
        CompareResult result = compareService.compareTableStructures(sourceTable, targetTable, compareConfig);

        // 验证结果
        assertNotNull(result);
        assertTrue(result.getColumnDifferences().size() > 0, "应该检测到列差异");
        assertTrue(result.getIndexDifferences().size() > 0, "应该检测到索引差异");
        
        // 验证时间字段的差异被检测到
        boolean foundTimeDifference = false;
        for (CompareResult.ColumnDifference diff : result.getColumnDifferences()) {
            if ("create_time".equals(diff.getColumnName()) || "update_time".equals(diff.getColumnName())) {
                foundTimeDifference = true;
                break;
            }
        }
        assertTrue(foundTimeDifference, "应该检测到时间字段的差异");
    }

    @Test
    public void testSpecialCaseHandling_MySqlAndTidb() throws Exception {
        // 准备测试数据 - 源表配置 (MySQL)
        DataSourceConfig.TableConfig sourceTableConfig = new DataSourceConfig.TableConfig();
        sourceTableConfig.setType("mysql");
        sourceTableConfig.setDataSourceName("mysqlDataSource");
        sourceTableConfig.setTableName("mysql_table");

        // 准备测试数据 - 目标表配置 (TiDB)
        DataSourceConfig.TableConfig targetTableConfig = new DataSourceConfig.TableConfig();
        targetTableConfig.setType("tidb");
        targetTableConfig.setDataSourceName("tidbDataSource");
        targetTableConfig.setTableName("tidb_table");

        // 获取表结构
        TableStructure sourceTable = compareService.getTableStructure(sourceTableConfig);
        TableStructure targetTable = compareService.getTableStructure(targetTableConfig);
        
        // 模拟TiDB的AUTO_RANDOM属性
        for (ColumnStructure column : targetTable.getColumns()) {
            if ("id".equals(column.getColumnName())) {
                Map<String, Object> properties = new HashMap<>();
                properties.put("is_auto_random", true);
                column.setProperties(properties);
                column.setAutoIncrement(false); // TiDB中标记为AUTO_RANDOM而不是AUTO_INCREMENT
                break;
            }
        }

        // 设置比对配置
        DataSourceConfig.CompareConfig compareConfig = new DataSourceConfig.CompareConfig();
        compareConfig.setName("test-mysql-tidb-special");

        // 执行比对
        CompareResult result = compareService.compareTableStructures(sourceTable, targetTable, compareConfig);

        // 验证结果
        assertNotNull(result);
        
        // 检查AUTO_INCREMENT和AUTO_RANDOM的特殊处理
        // 在MySQL源表中id列是AUTO_INCREMENT，在TiDB目标表中是AUTO_RANDOM
        // 这个差异应该被特殊处理，不算作差异
        boolean autoIncrementDiffFound = false;
        for (CompareResult.ColumnDifference diff : result.getColumnDifferences()) {
            if ("id".equals(diff.getColumnName())) {
                for (Map.Entry<String, CompareResult.PropertyDifference> propDiff : diff.getPropertyDifferences().entrySet()) {
                    if ("autoIncrement".equals(propDiff.getKey())) {
                        autoIncrementDiffFound = true;
                        break;
                    }
                }
            }
        }
        
        assertFalse(autoIncrementDiffFound, "MySQL的AUTO_INCREMENT和TiDB的AUTO_RANDOM不应该被视为差异");
    }

    @Test
    public void testIgnoreColumnTypeDifferences() throws Exception {
        // 准备测试数据 - 源表配置
        DataSourceConfig.TableConfig sourceTableConfig = new DataSourceConfig.TableConfig();
        sourceTableConfig.setType("mysql");
        sourceTableConfig.setDataSourceName("mysqlDataSource");
        sourceTableConfig.setTableName("mysql_table");

        // 准备测试数据 - 目标表配置
        DataSourceConfig.TableConfig targetTableConfig = new DataSourceConfig.TableConfig();
        targetTableConfig.setType("tidb");
        targetTableConfig.setDataSourceName("tidbDataSource");
        targetTableConfig.setTableName("tidb_table");

        // 获取表结构
        TableStructure sourceTable = compareService.getTableStructure(sourceTableConfig);
        TableStructure targetTable = compareService.getTableStructure(targetTableConfig);

        // 设置比对配置 - 忽略长度和精度差异
        DataSourceConfig.CompareConfig compareConfig = new DataSourceConfig.CompareConfig();
        compareConfig.setName("test-ignore-types");
        compareConfig.setIgnoreTypes(Arrays.asList("LENGTH", "PRECISION", "SCALE"));

        // 执行比对
        CompareResult result = compareService.compareTableStructures(sourceTable, targetTable, compareConfig);

        // 验证结果
        assertNotNull(result);
        
        // 验证忽略的类型差异
        boolean lengthDiffFound = false;
        boolean precisionDiffFound = false;
        
        for (CompareResult.ColumnDifference diff : result.getColumnDifferences()) {
            Map<String, CompareResult.PropertyDifference> propDiffs = diff.getPropertyDifferences();
            if (propDiffs.containsKey("length")) {
                lengthDiffFound = true;
            }
            if (propDiffs.containsKey("precision") || propDiffs.containsKey("scale")) {
                precisionDiffFound = true;
            }
        }
        
        assertFalse(lengthDiffFound, "长度差异应该被忽略");
        assertFalse(precisionDiffFound, "精度差异应该被忽略");
        
        // 重新执行比对，这次不忽略任何类型
        compareConfig.setIgnoreTypes(new ArrayList<>());
        CompareResult fullResult = compareService.compareTableStructures(sourceTable, targetTable, compareConfig);
        
        // 这次应该检测到长度和精度差异
        boolean foundDiff = false;
        for (CompareResult.ColumnDifference diff : fullResult.getColumnDifferences()) {
            Map<String, CompareResult.PropertyDifference> propDiffs = diff.getPropertyDifferences();
            if (propDiffs.containsKey("length") || propDiffs.containsKey("precision") || propDiffs.containsKey("scale")) {
                foundDiff = true;
                break;
            }
        }
        
        assertTrue(foundDiff, "未忽略时应该检测到列的长度或精度差异");
    }

    /**
     * 通过反射设置字段值
     */
    private void setFieldByReflection(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Error setting field by reflection", e);
        }
    }
} 