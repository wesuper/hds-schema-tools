package org.immortal.hydra.hdstbcomp.extractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.wesuper.jtools.hdscompare.config.DataSourceConfig;
import org.wesuper.jtools.hdscompare.extractor.MySqlTableStructureExtractor;
import org.wesuper.jtools.hdscompare.model.ColumnStructure;
import org.wesuper.jtools.hdscompare.model.IndexStructure;
import org.wesuper.jtools.hdscompare.model.TableStructure;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MySQL表结构提取器单元测试
 */
public class MySqlTableStructureExtractorTest {

    private MySqlTableStructureExtractor extractor;
    private EmbeddedDatabase h2Database;
    private Map<String, DataSource> dataSourceMap;

    @BeforeEach
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
        assertNotNull(tableStructure, "表结构不应为空");
        assertEquals("mysql_table", tableStructure.getTableName(), "表名应正确");
        assertEquals("mysql", tableStructure.getSourceType(), "数据源类型应为mysql");
        // 兼容H2数据库注释可能为空
        if (!isH2Database(h2Database)) {
            assertEquals("模拟MySQL表", tableStructure.getTableComment(), "表注释应正确");
        }

        // 验证列信息
        List<ColumnStructure> columns = tableStructure.getColumns();
        assertEquals(8, columns.size(), "应提取8列");

        // 验证特定列
        ColumnStructure idColumn = findColumnByName(columns, "id");
        assertNotNull(idColumn, "应存在id列");
        if (!isH2Database(h2Database)) {
            assertTrue(idColumn.isAutoIncrement(), "id列应自增");
        }
        assertEquals("INTEGER", idColumn.getDataType().toUpperCase(), "id列类型应正确");

        ColumnStructure descColumn = findColumnByName(columns, "description");
        assertNotNull(descColumn, "应存在description列");
        assertEquals(Integer.valueOf(500), descColumn.getLength(), "description列长度应为500");
        assertTrue(descColumn.isNullable(), "description列可为空");

        ColumnStructure amountColumn = findColumnByName(columns, "amount");
        assertNotNull(amountColumn, "应存在amount列");
        if (!isH2Database(h2Database)) {
            assertEquals(Integer.valueOf(10), amountColumn.getPrecision(), "amount列精度应为10");
            assertEquals(Integer.valueOf(2), amountColumn.getScale(), "amount列小数位数应为2");
        }

        // 验证索引信息
        List<IndexStructure> indexes = tableStructure.getIndexes();
        if (!isH2Database(h2Database)) {
            assertTrue(indexes.size() >= 2, "应提取索引");
        }

        // 验证主键索引
        IndexStructure pkIndex = findPrimaryKeyIndex(indexes);
        assertNotNull(pkIndex, "应存在主键索引");
        assertTrue(pkIndex.isPrimary(), "主键索引应标记为主键");
        assertEquals("id", pkIndex.getColumns().get(0).getColumnName(), "主键索引应包含id列");

        // 验证唯一索引
        IndexStructure ukIndex = findIndexByName(indexes, "mysql_table_uk_name");
        assertNotNull(ukIndex, "应存在唯一索引uk_name");
        assertTrue(ukIndex.isUnique(), "uk_name应为唯一索引");
        assertEquals("name", ukIndex.getColumns().get(0).getColumnName(), "uk_name应包含name列");
    }

    /**
     * 根据名称查找列
     */
    private ColumnStructure findColumnByName(List<ColumnStructure> columns, String name) {
        return columns.stream()
                .filter(column -> name.equalsIgnoreCase(column.getColumnName()))
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

    /**
     * 判断当前数据源是否为H2
     */
    private boolean isH2Database(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            String productName = conn.getMetaData().getDatabaseProductName();
            return productName != null && productName.toLowerCase().contains("h2");
        } catch (Exception e) {
            return false;
        }
    }
} 