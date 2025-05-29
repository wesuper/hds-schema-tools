package org.wesuper.jtools.hdscompare.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.lookup.DataSourceLookupFailureException;
import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.constants.DatabaseType;
import org.wesuper.jtools.hdscompare.model.ColumnStructure;
import org.wesuper.jtools.hdscompare.model.IndexStructure;
import org.wesuper.jtools.hdscompare.model.TableStructure;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MySQL表结构提取器实现
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class MySqlTableStructureExtractor implements TableStructureExtractor {

    private static final Logger logger = LoggerFactory.getLogger(MySqlTableStructureExtractor.class);
    
    private static final String TYPE = DatabaseType.MYSQL;
    
    /**
     * 获取表注释的SQL
     */
    private static final String TABLE_COMMENT_SQL = 
            "SELECT table_comment FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
    
    /**
     * 获取列详细信息的SQL
     */
    private static final String COLUMN_DETAILS_SQL = 
        "SELECT column_name, data_type, column_type, column_default, is_nullable, " +
        "character_maximum_length, numeric_precision, numeric_scale, column_comment, ordinal_position, extra " +
        "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
    
    @Autowired
    private Map<String, DataSource> dataSourceMap;
    
    @Override
    public TableStructure extractTableStructure(DataSourceCompareConfig.DataSourceConfig dataSourceConfig, String tableName) throws Exception {
        String dataSourceName = dataSourceConfig.getDataSourceName();
        
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
        } catch(EmptyResultDataAccessException e){
            logger.info("table {} does not have comment", tableName);
            return "";
        }
        catch (Exception e) {
            logger.warn("Failed to get table comment for {}", tableName, e);
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
        return jdbcTemplate.query(COLUMN_DETAILS_SQL, getColumnStructureRowMapper(), schema, tableName);
    }
    
    /**
     * 创建列结构行映射器
     * 
     * @return 列结构行映射器
     */
    private RowMapper<ColumnStructure> getColumnStructureRowMapper() {
        return (rs, rowNum) -> {
            ColumnStructure column = new ColumnStructure();
            column.setColumnName(rs.getString("column_name"));
            column.setDataType(rs.getString("data_type"));
            column.setColumnType(rs.getString("column_type"));
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
            
            String extra = rs.getString("extra");
            column.setAutoIncrement(extra != null && extra.toLowerCase(java.util.Locale.ROOT).contains("auto_increment"));
            
            Map<String, Object> properties = new HashMap<>();
            properties.put("extra", rs.getString("extra"));
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
        
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            // 获取索引信息
            try (ResultSet rs = metaData.getIndexInfo(catalog, null, tableName, false, true)) {
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
            try (ResultSet rs = metaData.getPrimaryKeys(catalog, null, tableName)) {
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