package org.immortal.hydra.hdstbcomp.extractor;

import org.immortal.hydra.hdstbcomp.config.DataSourceConfig;
import org.immortal.hydra.hdstbcomp.model.ColumnStructure;
import org.immortal.hydra.hdstbcomp.model.IndexStructure;
import org.immortal.hydra.hdstbcomp.model.TableStructure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.lookup.DataSourceLookupFailureException;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Component;
import org.springframework.jdbc.core.ConnectionCallback;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * MySQL表结构提取器实现
 * 
 * @author vincentruan
 * @version 1.0.0
 */
@Component
public class MySqlTableStructureExtractor implements TableStructureExtractor {

    private static final Logger logger = LoggerFactory.getLogger(MySqlTableStructureExtractor.class);
    
    private static final String TYPE = "mysql";
    
    /**
     * 获取表注释的SQL
     */
    private static final String TABLE_COMMENT_SQL = 
            "SELECT remarks FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
    
    /**
     * 获取列详细信息的SQL
     */
    private static final String MYSQL_COLUMN_DETAILS_SQL = 
        "SELECT column_name, data_type, column_type, column_default, is_nullable, " +
        "character_maximum_length, numeric_precision, numeric_scale, column_comment, ordinal_position, extra " +
        "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
            
    /**
     * 获取列详细信息的SQL (H2)
     */
    private static final String H2_COLUMN_DETAILS_SQL = 
        "SELECT column_name, data_type, column_default, is_nullable, " +
        "character_maximum_length, numeric_precision, numeric_scale, remarks as column_comment, ordinal_position " +
        "FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position";
    
    @Autowired
    private Map<String, DataSource> dataSourceMap;
    
    @Override
    public TableStructure extractTableStructure(DataSourceConfig.TableConfig tableConfig) throws Exception {
        String dataSourceName = tableConfig.getDataSourceName();
        String tableName = tableConfig.getTableName();
        
        logger.info("Extracting structure for MySQL table: {} from datasource: {}", tableName, dataSourceName);
        
        DataSource dataSource = getDataSource(dataSourceName);
        if (dataSource == null) {
            throw new DataSourceLookupFailureException("DataSource not found: " + dataSourceName);
        }
        
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        
        TableStructure tableStructure = new TableStructure();
        tableStructure.setTableName(tableName);
        tableStructure.setSourceType(TYPE);
        
        try {
            // 提取数据库名
            String catalog = getCatalog(dataSource);
            
            // 提取表注释
            tableStructure.setTableComment(getTableComment(jdbcTemplate, catalog, tableName));
            
            // 提取列信息
            tableStructure.setColumns(getColumnStructures(jdbcTemplate, catalog, tableName));
            
            // 提取索引信息
            tableStructure.setIndexes(getIndexStructures(dataSource, catalog, tableName));
            
            return tableStructure;
        } catch (Exception e) {
            logger.error("Failed to extract MySQL table structure for {}: {}", tableName, e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public String getSupportedType() {
        return TYPE;
    }
    
    /**
     * 获取表注释
     * 
     * @param jdbcTemplate JDBC模板
     * @param schema 数据库名
     * @param tableName 表名
     * @return 表注释
     */
    private String getTableComment(JdbcTemplate jdbcTemplate, String schema, String tableName) {
        try {
            return jdbcTemplate.queryForObject(TABLE_COMMENT_SQL, String.class, schema, tableName);
        } catch (Exception e) {
            logger.warn("Failed to get table comment for {}: {}", tableName, e.getMessage());
            return "";
        }
    }
    
    /**
     * 获取列结构列表
     * 
     * @param jdbcTemplate JDBC模板
     * @param schema 数据库名
     * @param tableName 表名
     * @return 列结构列表
     */
    private List<ColumnStructure> getColumnStructures(JdbcTemplate jdbcTemplate, String schema, String tableName) {
        boolean isH2 = isH2Database(jdbcTemplate);
        String sql = isH2 ? H2_COLUMN_DETAILS_SQL : MYSQL_COLUMN_DETAILS_SQL;
        if (isH2) {
            return jdbcTemplate.query(sql, new Object[]{tableName.toUpperCase()}, getColumnStructureRowMapper(true));
        } else {
            return jdbcTemplate.query(sql, new Object[]{schema, tableName}, getColumnStructureRowMapper(false));
        }
    }
    
    /**
     * 判断是否为H2数据库
     */
    private boolean isH2Database(JdbcTemplate jdbcTemplate) {
        try {
            // 使用DatabaseMetaData来判断数据库类型
            return jdbcTemplate.execute((ConnectionCallback<Boolean>) connection -> {
                DatabaseMetaData metaData = connection.getMetaData();
                String databaseProductName = metaData.getDatabaseProductName();
                return databaseProductName != null && 
                       databaseProductName.toLowerCase().contains("h2");
            });
        } catch (Exception e) {
            logger.warn("Failed to determine database type: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 创建列结构行映射器
     * 
     * @param isH2 是否为H2数据库
     * @return 列结构行映射器
     */
    private RowMapper<ColumnStructure> getColumnStructureRowMapper(boolean isH2) {
        return (rs, rowNum) -> {
            ColumnStructure column = new ColumnStructure();
            column.setColumnName(rs.getString("column_name"));
            column.setDataType(rs.getString("data_type"));
            if (isH2) {
                String dataType = rs.getString("data_type").toLowerCase(java.util.Locale.ROOT);
                String columnType = dataType;
                if (dataType.contains("char") || dataType.contains("text") || dataType.contains("binary") || dataType.contains("blob")) {
                    Integer length = rs.getObject("character_maximum_length") != null ? rs.getInt("character_maximum_length") : null;
                    if (length != null) {
                        columnType += "(" + length + ")";
                    }
                } else if (dataType.contains("int") || dataType.contains("float") || dataType.contains("double") || dataType.contains("decimal")) {
                    Integer precision = rs.getObject("numeric_precision") != null ? rs.getInt("numeric_precision") : null;
                    Integer scale = rs.getObject("numeric_scale") != null ? rs.getInt("numeric_scale") : null;
                    if (precision != null) {
                        columnType += "(" + precision;
                        if (scale != null) {
                            columnType += "," + scale;
                        }
                        columnType += ")";
                    }
                }
                column.setColumnType(columnType);
            } else {
                column.setColumnType(rs.getString("column_type"));
            }
            column.setDefaultValue(rs.getString("column_default"));
            column.setNullable("YES".equalsIgnoreCase(rs.getString("is_nullable")));
            String dataType = rs.getString("data_type").toLowerCase(java.util.Locale.ROOT);
            if (dataType.contains("char") || dataType.contains("text") || dataType.contains("binary") || dataType.contains("blob")) {
                column.setLength(rs.getObject("character_maximum_length") != null ? rs.getInt("character_maximum_length") : null);
            } else if (dataType.contains("int") || dataType.contains("float") || dataType.contains("double") || dataType.contains("decimal")) {
                column.setPrecision(rs.getObject("numeric_precision") != null ? rs.getInt("numeric_precision") : null);
                column.setScale(rs.getObject("numeric_scale") != null ? rs.getInt("numeric_scale") : null);
            }
            column.setComment(rs.getString("column_comment"));
            column.setOrdinalPosition(rs.getInt("ordinal_position"));
            if (isH2) {
                boolean autoIncrement = false;
                try {
                    String dataTypeLocal = rs.getString("data_type");
                    if (dataTypeLocal != null) {
                        String type = dataTypeLocal.toUpperCase();
                        if (type.contains("IDENTITY") || type.contains("AUTO_INCREMENT")) {
                            autoIncrement = true;
                        }
                    }
                } catch (Exception ignore) {
                    // H2没有该字段，默认false
                }
                column.setAutoIncrement(autoIncrement);
            } else {
                String extra = rs.getString("extra");
                column.setAutoIncrement(extra != null && extra.toLowerCase(java.util.Locale.ROOT).contains("auto_increment"));
            }
            Map<String, Object> properties = new HashMap<>();
            if (!isH2) {
                properties.put("extra", rs.getString("extra"));
            }
            column.setProperties(properties);
            return column;
        };
    }
    
    /**
     * 获取索引结构列表
     * 
     * @param dataSource 数据源
     * @param catalog 数据库名
     * @param tableName 表名
     * @return 索引结构列表
     * @throws SQLException SQL异常
     */
    private List<IndexStructure> getIndexStructures(DataSource dataSource, String catalog, String tableName) throws SQLException {
        Map<String, IndexStructure> indexMap = new HashMap<>();
        boolean isH2 = false;
        
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            String databaseProductName = metaData.getDatabaseProductName();
            isH2 = databaseProductName != null && databaseProductName.toLowerCase().contains("h2");
            String actualTableName = isH2 ? tableName.toUpperCase() : tableName;
            
            // 获取索引信息
            try (ResultSet rs = metaData.getIndexInfo(catalog, null, actualTableName, false, true)) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    
                    // 跳过统计信息
                    if (indexName == null) {
                        continue;
                    }
                    
                    // 创建或获取索引结构
                    IndexStructure indexStructure = indexMap.computeIfAbsent(indexName, k -> {
                        IndexStructure is = new IndexStructure();
                        is.setIndexName(indexName);
                        is.setPrimary("PRIMARY".equalsIgnoreCase(indexName));
                        try {
                            is.setUnique(!rs.getBoolean("NON_UNIQUE"));
                            short indexType = rs.getShort("TYPE");
                            is.setIndexType(indexType == DatabaseMetaData.tableIndexStatistic ? "STATISTIC" : "NORMAL");
                        } catch (SQLException e) {
                            logger.warn("Error reading index metadata for {}: {}", indexName, e.getMessage());
                            is.setUnique(false);
                            is.setIndexType("NORMAL");
                        }
                        return is;
                    });
                    
                    // 获取列信息
                    String columnName = rs.getString("COLUMN_NAME");
                    if (columnName != null) {
                        IndexStructure.IndexColumnStructure columnStructure = new IndexStructure.IndexColumnStructure();
                        columnStructure.setColumnName(columnName.toLowerCase());
                        columnStructure.setPosition(rs.getShort("ORDINAL_POSITION"));
                        columnStructure.setSort(rs.getString("ASC_OR_DESC"));
                        indexStructure.getColumns().add(columnStructure);
                    }
                }
            }
            
            // 获取主键信息
            try (ResultSet rs = metaData.getPrimaryKeys(catalog, null, actualTableName)) {
                List<String> pkColumns = new ArrayList<>();
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    if (columnName != null) {
                        pkColumns.add(columnName);
                    }
                }
                // 如果存在主键列，强制创建主键索引（不管indexMap里有没有"PRIMARY"）
                if (!pkColumns.isEmpty()) {
                    IndexStructure pkIndexStructure = new IndexStructure();
                    pkIndexStructure.setIndexName("PRIMARY");
                    pkIndexStructure.setPrimary(true);
                    pkIndexStructure.setUnique(true);
                    pkIndexStructure.setIndexType("PRIMARY KEY");
                    for (int i = 0; i < pkColumns.size(); i++) {
                        IndexStructure.IndexColumnStructure columnStructure = new IndexStructure.IndexColumnStructure();
                        columnStructure.setColumnName(pkColumns.get(i).toLowerCase());
                        columnStructure.setPosition((short) (i + 1));
                        pkIndexStructure.getColumns().add(columnStructure);
                    }
                    // 直接put，覆盖任何同名索引
                    indexMap.put("PRIMARY", pkIndexStructure);
                }
            }
        }
        // 调试输出所有索引
        logger.debug("DEBUG: indexMap = {}", indexMap);
        return new ArrayList<>(indexMap.values());
    }
    
    /**
     * 获取数据库名
     * 
     * @param dataSource 数据源
     * @return 数据库名
     * @throws SQLException SQL异常
     */
    protected String getCatalog(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return conn.getCatalog();
        }
    }
    
    /**
     * 获取数据源
     * 
     * @param dataSourceName 数据源名称
     * @return 数据源
     */
    protected DataSource getDataSource(String dataSourceName) {
        DataSource dataSource = dataSourceMap.get(dataSourceName);
        if (dataSource == null) {
            logger.error("DataSource not found: {}", dataSourceName);
        }
        return dataSource;
    }
} 