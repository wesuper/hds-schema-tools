package org.immortal.hydra.hdstbcomp.extractor;

import org.immortal.hydra.hdstbcomp.config.DataSourceConfig;
import org.immortal.hydra.hdstbcomp.model.IndexStructure;
import org.immortal.hydra.hdstbcomp.model.TableStructure;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.lookup.DataSourceLookupFailureException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * MySQL表结构提取器错误处理单元测试
 */
@RunWith(MockitoJUnitRunner.class)
public class MySqlTableStructureExtractorErrorHandlingTest {

    @InjectMocks
    private MySqlTableStructureExtractor extractor;

    @Mock
    private Map<String, DataSource> dataSourceMap;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

    @Mock
    private ResultSet indexResultSet;

    @Before
    public void setup() throws Exception {
        // 设置基本的mock行为
        when(dataSourceMap.get(anyString())).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(connection.getCatalog()).thenReturn("test_catalog");
        
        // Mock索引结果集
        when(databaseMetaData.getIndexInfo(anyString(), anyString(), anyString(), anyBoolean(), anyBoolean()))
                .thenReturn(indexResultSet);
        when(databaseMetaData.getPrimaryKeys(anyString(), anyString(), anyString()))
                .thenReturn(mock(ResultSet.class));
                
        // 模拟JdbcTemplate的行为
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        setFieldByReflection(extractor, "jdbcTemplate", jdbcTemplate);
    }

    @Test
    public void testSQLExceptionHandlingInIndexExtraction() throws Exception {
        // 设置mock行为，模拟索引提取过程中的SQLException
        when(indexResultSet.next()).thenReturn(true, false); // 只有一行数据
        when(indexResultSet.getString("INDEX_NAME")).thenReturn("test_index");
        
        // 第一次获取NON_UNIQUE时抛出SQLException，模拟数据库元数据访问失败
        when(indexResultSet.getBoolean("NON_UNIQUE"))
                .thenThrow(new SQLException("Test SQL exception in index extraction"));
        
        // 创建表配置
        DataSourceConfig.TableConfig tableConfig = new DataSourceConfig.TableConfig();
        tableConfig.setType("mysql");
        tableConfig.setDataSourceName("dataSource");
        tableConfig.setTableName("test_table");
        
        // 提取表结构 - 应该能处理异常并继续
        TableStructure tableStructure = extractor.extractTableStructure(tableConfig);
        
        // 验证即使有异常，也应该有索引数据
        List<IndexStructure> indexes = tableStructure.getIndexes();
        assertNotNull("即使有异常，索引列表也不应为空", indexes);
        
        // 查找测试索引
        IndexStructure testIndex = indexes.stream()
                .filter(idx -> "test_index".equals(idx.getIndexName()))
                .findFirst()
                .orElse(null);
        
        assertNotNull("应该提取到测试索引", testIndex);
        // 验证默认值是否正确设置 (在异常处理中应该设置这些值)
        assertFalse("异常时unique应默认设为false", testIndex.isUnique());
        assertEquals("异常时索引类型应默认为NORMAL", "NORMAL", testIndex.getIndexType());
    }

    @Test(expected = DataSourceLookupFailureException.class)
    public void testDataSourceNotFound() throws Exception {
        // 模拟数据源未找到的情况
        when(dataSourceMap.get(anyString())).thenReturn(null);
        
        // 创建表配置
        DataSourceConfig.TableConfig tableConfig = new DataSourceConfig.TableConfig();
        tableConfig.setType("mysql");
        tableConfig.setDataSourceName("nonexistent");
        tableConfig.setTableName("test_table");
        
        // 尝试提取表结构 - 应该抛出DataSourceLookupFailureException
        extractor.extractTableStructure(tableConfig);
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