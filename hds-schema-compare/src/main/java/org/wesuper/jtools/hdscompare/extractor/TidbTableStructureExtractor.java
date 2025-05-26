package org.wesuper.jtools.hdscompare.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.wesuper.jtools.hdscompare.config.DataSourceConfig;
import org.wesuper.jtools.hdscompare.model.ColumnStructure;
import org.wesuper.jtools.hdscompare.model.TableStructure;

import java.util.Map;

/**
 * TiDB表结构提取器实现
 * 由于TiDB与MySQL接口兼容，因此大部分逻辑可以复用MySQL实现，
 * 同时处理TiDB特有的属性
 * 
 * @author vincentruan
 * @version 1.0.0
 */
@Component
public class TidbTableStructureExtractor extends MySqlTableStructureExtractor {

    private static final Logger logger = LoggerFactory.getLogger(TidbTableStructureExtractor.class);
    
    private static final String TYPE = "tidb";
    
    /**
     * TiDB特有的属性查询SQL，从系统表中查询额外信息
     */
    private static final String TIDB_TABLE_ATTRIBUTES_SQL = 
            "SELECT tidb_pk_type, tidb_row_id_sharding_info FROM information_schema.tables " +
            "WHERE table_schema = ? AND table_name = ?";
    
    @Override
    public TableStructure extractTableStructure(DataSourceConfig.TableConfig tableConfig) throws Exception {
        // 先通过MySQL提取器获取基本结构
        TableStructure tableStructure = super.extractTableStructure(tableConfig);
        
        // 设置正确的源类型
        tableStructure.setSourceType(TYPE);
        
        try {
            // 获取TiDB特有属性
            appendTidbSpecificAttributes(tableConfig, tableStructure);
        } catch (Exception e) {
            logger.warn("Failed to extract TiDB specific attributes for table {}: {}", 
                    tableConfig.getTableName(), e.getMessage());
        }
        
        return tableStructure;
    }
    
    @Override
    public String getSupportedType() {
        return TYPE;
    }
    
    /**
     * 添加TiDB特有的表属性
     * 
     * @param tableConfig 表配置
     * @param tableStructure 表结构
     */
    private void appendTidbSpecificAttributes(DataSourceConfig.TableConfig tableConfig, TableStructure tableStructure) {
        // 设置TiDB特有属性
        try {
            String dataSourceName = tableConfig.getDataSourceName();
            String tableName = tableConfig.getTableName();
            
            JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource(dataSourceName));
            String catalog = getCatalog(getDataSource(dataSourceName));
            
            // 查询TiDB特有的表属性
            jdbcTemplate.query(TIDB_TABLE_ATTRIBUTES_SQL, 
                new Object[] { catalog, tableName }, 
                rs -> {
                    Map<String, Object> properties = tableStructure.getProperties();
                    
                    // 提取AUTO_RANDOM信息 (TiDB特有的自增类型)
                    String pkType = rs.getString("tidb_pk_type");
                    if (pkType != null && !pkType.isEmpty()) {
                        properties.put("tidb_pk_type", pkType);
                        
                        // 如果是AUTO_RANDOM，需要标记对应的列
                        if ("AUTO_RANDOM".equalsIgnoreCase(pkType)) {
                            updateAutoRandomColumn(tableStructure);
                        }
                    }
                    
                    // 行ID分片信息
                    String rowIdSharding = rs.getString("tidb_row_id_sharding_info");
                    if (rowIdSharding != null && !rowIdSharding.isEmpty()) {
                        properties.put("tidb_row_id_sharding", rowIdSharding);
                    }
                    return null;
                }
            );
        } catch (RuntimeException | java.sql.SQLException e) {
            logger.warn("Error fetching TiDB specific attributes for table {}: {}", 
                    tableConfig.getTableName(), e.getMessage());
        }
    }
    
    /**
     * 更新具有AUTO_RANDOM属性的列
     * 
     * @param tableStructure 表结构
     */
    private void updateAutoRandomColumn(TableStructure tableStructure) {
        // 在TiDB中，AUTO_RANDOM只能应用于主键列
        // 找到主键列并设置AUTO_RANDOM标志
        tableStructure.getIndexes().stream()
            .filter(index -> index.isPrimary())
            .findFirst()
            .ifPresent(pkIndex -> {
                if (!pkIndex.getColumns().isEmpty()) {
                    String pkColumnName = pkIndex.getColumns().get(0).getColumnName();
                    ColumnStructure pkColumn = tableStructure.getColumnByName(pkColumnName);
                    
                    if (pkColumn != null) {
                        pkColumn.getProperties().put("is_auto_random", true);
                        logger.info("Column {} is marked as AUTO_RANDOM", pkColumnName);
                    }
                }
            });
    }
} 