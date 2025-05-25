package org.immortal.hydra.hdstbcomp.extractor;

import org.immortal.hydra.hdstbcomp.config.DataSourceConfig;
import org.immortal.hydra.hdstbcomp.model.ColumnStructure;
import org.immortal.hydra.hdstbcomp.model.IndexStructure;
import org.immortal.hydra.hdstbcomp.model.TableStructure;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * MySQL表结构提取器单元测试
 */
public class MySqlTableStructureExtractorTest {

    private MySqlTableStructureExtractor extractor;
    private EmbeddedDatabase h2Database;
    private Map<String, DataSource> dataSourceMap;

    @Before
    public void setup() {
        // 创建H2嵌入式数据库
        h2Database = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:schema.sql")
                .build();

        // 创建数据源映射
        dataSourceMap = new HashMap<>();
        dataSourceMap.put("h2DataSource", h2Database);

        // 创建并配置MySQL提取器
        extractor = new MySqlTableStructureExtractor();
        setFieldByReflection(extractor, "dataSourceMap", dataSourceMap);
    }

    @Test
    public void testExtractTableStructure() throws Exception {
        // 创建表配置
        DataSourceConfig.TableConfig tableConfig = new DataSourceConfig.TableConfig();
        tableConfig.setType("mysql");
        tableConfig.setDataSourceName("h2DataSource");
        tableConfig.setTableName("mysql_table");

        // 提取表结构
        TableStructure tableStructure = extractor.extractTableStructure(tableConfig);

        // 验证基本信息
        assertNotNull("表结构不应为空", tableStructure);
        assertEquals("表名应正确", "mysql_table", tableStructure.getTableName());
        assertEquals("数据源类型应为mysql", "mysql", tableStructure.getSourceType());
        assertEquals("表注释应正确", "模拟MySQL表", tableStructure.getTableComment());

        // 验证列信息
        List<ColumnStructure> columns = tableStructure.getColumns();
        assertEquals("应提取8列", 8, columns.size());

        // 验证特定列
        ColumnStructure idColumn = findColumnByName(columns, "id");
        assertNotNull("应存在id列", idColumn);
        assertTrue("id列应自增", idColumn.isAutoIncrement());
        assertEquals("id列类型应正确", "INTEGER", idColumn.getDataType().toUpperCase());

        ColumnStructure descColumn = findColumnByName(columns, "description");
        assertNotNull("应存在description列", descColumn);
        assertEquals("description列长度应为500", Integer.valueOf(500), descColumn.getLength());
        assertTrue("description列可为空", descColumn.isNullable());

        ColumnStructure amountColumn = findColumnByName(columns, "amount");
        assertNotNull("应存在amount列", amountColumn);
        assertEquals("amount列精度应为10", Integer.valueOf(10), amountColumn.getPrecision());
        assertEquals("amount列小数位数应为2", Integer.valueOf(2), amountColumn.getScale());

        // 验证索引信息
        List<IndexStructure> indexes = tableStructure.getIndexes();
        assertTrue("应提取索引", indexes.size() >= 2);

        // 验证主键索引
        IndexStructure pkIndex = findPrimaryKeyIndex(indexes);
        assertNotNull("应存在主键索引", pkIndex);
        assertTrue("主键索引应标记为主键", pkIndex.isPrimary());
        assertEquals("主键索引应包含id列", "id", pkIndex.getColumns().get(0).getColumnName());

        // 验证唯一索引
        IndexStructure ukIndex = findIndexByName(indexes, "uk_name");
        assertNotNull("应存在唯一索引uk_name", ukIndex);
        assertTrue("uk_name应为唯一索引", ukIndex.isUnique());
        assertEquals("uk_name应包含name列", "name", ukIndex.getColumns().get(0).getColumnName());
    }

    /**
     * 根据名称查找列
     */
    private ColumnStructure findColumnByName(List<ColumnStructure> columns, String name) {
        return columns.stream()
                .filter(column -> name.equals(column.getColumnName()))
                .findFirst()
                .orElse(null);
    }

    /**
     * 查找主键索引
     */
    private IndexStructure findPrimaryKeyIndex(List<IndexStructure> indexes) {
        return indexes.stream()
                .filter(IndexStructure::isPrimary)
                .findFirst()
                .orElse(null);
    }

    /**
     * 根据名称查找索引
     */
    private IndexStructure findIndexByName(List<IndexStructure> indexes, String name) {
        return indexes.stream()
                .filter(index -> name.equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElse(null);
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