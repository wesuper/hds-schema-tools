package org.wesuper.jtools.hdscompare.extractor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.constants.DatabaseType;
import org.wesuper.jtools.hdscompare.model.ColumnStructure;
import org.wesuper.jtools.hdscompare.model.IndexStructure;
import org.wesuper.jtools.hdscompare.model.TableStructure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch索引结构提取器实现
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class ElasticsearchTableStructureExtractor implements TableStructureExtractor {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTableStructureExtractor.class);
    
    private static final String TYPE = DatabaseType.ELASTICSEARCH;
    
    private final Map<String, RestHighLevelClient> elasticsearchClientMap;
    
    // ES类型到MySQL类型的映射 (使用Multimap)
    private static final Multimap<String, String> ES_TO_MYSQL_TYPE_MAPPING = ArrayListMultimap.create();
    
    static {
        // 数值类型映射
        ES_TO_MYSQL_TYPE_MAPPING.put("long", "bigint");
        ES_TO_MYSQL_TYPE_MAPPING.put("integer", "int");
        ES_TO_MYSQL_TYPE_MAPPING.put("short", "smallint");
        ES_TO_MYSQL_TYPE_MAPPING.put("byte", "tinyint");
        ES_TO_MYSQL_TYPE_MAPPING.put("float", "float");
        ES_TO_MYSQL_TYPE_MAPPING.put("double", "double");
        ES_TO_MYSQL_TYPE_MAPPING.put("scaled_float", "decimal");
        
        // 字符串类型映射
        ES_TO_MYSQL_TYPE_MAPPING.put("keyword", "varchar");
        ES_TO_MYSQL_TYPE_MAPPING.put("keyword", "char");
        ES_TO_MYSQL_TYPE_MAPPING.put("keyword", "enum"); // keyword can map to enum
        ES_TO_MYSQL_TYPE_MAPPING.put("text", "text");
        ES_TO_MYSQL_TYPE_MAPPING.put("text", "longtext");
        ES_TO_MYSQL_TYPE_MAPPING.put("text", "mediumtext");
        ES_TO_MYSQL_TYPE_MAPPING.put("text", "tinytext");
        ES_TO_MYSQL_TYPE_MAPPING.put("text", "varchar"); // Add mapping from text to varchar
        
        // 日期时间类型映射
        ES_TO_MYSQL_TYPE_MAPPING.put("date", "datetime");
        ES_TO_MYSQL_TYPE_MAPPING.put("date", "timestamp");
        ES_TO_MYSQL_TYPE_MAPPING.put("date", "date");
        
        // 布尔类型映射
        ES_TO_MYSQL_TYPE_MAPPING.put("boolean", "boolean");
        ES_TO_MYSQL_TYPE_MAPPING.put("boolean", "bool"); // MySQL also supports 'bool'
    }
    
    // @Autowired // Assuming this is handled by Spring configuration
    public ElasticsearchTableStructureExtractor(Map<String, RestHighLevelClient> elasticsearchClientMap) {
        this.elasticsearchClientMap = elasticsearchClientMap;
    }
    
    @Override
    public TableStructure extractTableStructure(DataSourceCompareConfig.DataSourceConfig dataSourceConfig, String tableName) throws Exception {
        String dataSourceName = dataSourceConfig.getDataSourceName();
        String indexName = tableName;
        
        logger.info("Extracting structure for Elasticsearch index: {} from datasource: {}", indexName, dataSourceName);
        
        RestHighLevelClient client = getElasticsearchClient(dataSourceName);
        if (client == null) {
            throw new IllegalArgumentException("Elasticsearch client not found for data source: " + dataSourceName);
        }
        
        TableStructure tableStructure = new TableStructure();
        tableStructure.setTableName(indexName);
        tableStructure.setSourceType(TYPE);
        
        try {
            GetIndexRequest request = new GetIndexRequest().indices(indexName);
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
            
            Settings indexSettings = response.getSettings().get(indexName);
            if (indexSettings != null) {
                extractIndexSettings(tableStructure, indexSettings);
            }
            
            // In ES 7+, type is usually _doc. If not, try to get the first available mapping.
            ImmutableOpenMap<String, MappingMetadata> mappings = response.getMappings().get(indexName);
            String mappingTypeToUse = "_doc"; 
            if (mappings != null) {
                if (!mappings.containsKey(mappingTypeToUse) && !mappings.isEmpty()) {
                     mappingTypeToUse = mappings.keysIt().next(); // Get first type if _doc doesn't exist
                }
                MappingMetadata mappingMetaData = mappings.get(mappingTypeToUse);
                if (mappingMetaData != null) {
                    extractMappingMetadata(tableStructure, mappingMetaData);
                }
            } else {
                logger.warn("No mappings found for index: {}", indexName);
            }
            
            return tableStructure;
        } catch (Exception e) {
            logger.error("Failed to extract Elasticsearch index structure for {} from {}", indexName, dataSourceName, e);
            throw e;
        }
    }
    
    @Override
    public String getSupportedType() {
        return TYPE;
    }
    
    private void extractIndexSettings(TableStructure tableStructure, Settings settings) {
        Map<String, Object> properties = tableStructure.getProperties();
        properties.put("number_of_shards", settings.get("index.number_of_shards"));
        properties.put("number_of_replicas", settings.get("index.number_of_replicas"));
        properties.put("creation_date", settings.get("index.creation_date"));
        properties.put("uuid", settings.get("index.uuid"));
        properties.put("provided_name", settings.get("index.provided_name"));
        if (settings.get("index.analysis") != null) {
            properties.put("has_custom_analyzers", true);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void extractMappingMetadata(TableStructure tableStructure, MappingMetadata mappingMetaData) {
        try {
            Map<String, Object> mappingMap = mappingMetaData.sourceAsMap();
            Map<String, Object> fieldPropertiesMap = (Map<String, Object>) mappingMap.get("properties");
            
            if (fieldPropertiesMap != null) {
                int position = 1;
                for (Map.Entry<String, Object> entry : fieldPropertiesMap.entrySet()) {
                    String fieldName = entry.getKey();
                    Map<String, Object> esFieldProps = (Map<String, Object>) entry.getValue();
                    
                    ColumnStructure column = new ColumnStructure();
                    column.setColumnName(fieldName);
                    column.setOrdinalPosition(position++);
                    extractFieldProperties(column, esFieldProps);
                    tableStructure.getColumns().add(column);
                }
            }
            extractIndexInformation(tableStructure, mappingMap);
        } catch (Exception e) {
            logger.error("Error extracting Elasticsearch mapping metadata: {}", e.getMessage(), e);
        }
    }
    
    private void extractFieldProperties(ColumnStructure column, Map<String, Object> esFieldProps) {
        String esType = (String) esFieldProps.get("type");
        column.setDataType(esType); // Store original ES type as dataType
        column.setNullable(true); // In ES, fields are generally nullable by default
        
        Map<String, Object> columnProperties = new HashMap<>(esFieldProps); // Copy all ES props
        column.setProperties(columnProperties);
        
        // Add MySQL type mappings based on ES type
        for (String mysqlType : ES_TO_MYSQL_TYPE_MAPPING.get(esType)) {
            column.addTypeMapping(DatabaseType.MYSQL, mysqlType);
            // Also add for TiDB as it's MySQL compatible in this context
            column.addTypeMapping(DatabaseType.TIDB, mysqlType); 
        }
        
        if ("text".equals(esType) && esFieldProps.containsKey("analyzer")) {
            columnProperties.put("analyzer", esFieldProps.get("analyzer"));
        }
        if ("keyword".equals(esType) && esFieldProps.containsKey("ignore_above")) {
            column.setLength((Integer) esFieldProps.get("ignore_above"));
        }
        if (esType != null && (esType.equals("scaled_float")) && esFieldProps.containsKey("scaling_factor")) {
            columnProperties.put("scaling_factor", esFieldProps.get("scaling_factor"));
        }
    }
    
    @SuppressWarnings("unchecked")
    private void extractIndexInformation(TableStructure tableStructure, Map<String, Object> mappingMap) {
        List<IndexStructure> indexes = new ArrayList<>();
        if (mappingMap.containsKey("_id")) { /* ... existing _id handling ... */ }
        
        Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String fieldName = entry.getKey();
                Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
                Object indexFlag = fieldProps.get("index");
                boolean isIndexed = indexFlag == null || Boolean.TRUE.equals(indexFlag) || "true".equals(String.valueOf(indexFlag).toLowerCase());

                if (isIndexed && !fieldName.equals("_id")) { // Avoid duplicating _id index
                    IndexStructure fieldIndex = new IndexStructure();
                    fieldIndex.setIndexName(fieldName + "_idx"); // Simple naming convention
                    fieldIndex.setIndexType("NORMAL"); 
                    fieldIndex.setPrimary(false);
                    fieldIndex.setUnique(false); // ES non-ID indexes are generally not unique by default
                    
                    IndexStructure.IndexColumnStructure indexColumn = new IndexStructure.IndexColumnStructure();
                    indexColumn.setColumnName(fieldName);
                    indexColumn.setPosition(1);
                    List<IndexStructure.IndexColumnStructure> indexCols = new ArrayList<>();
                    indexCols.add(indexColumn);
                    fieldIndex.setColumns(indexCols);
                    
                    Map<String, Object> indexProps = new HashMap<>();
                    if (fieldProps.containsKey("analyzer")) {
                        indexProps.put("analyzer", fieldProps.get("analyzer"));
                    }
                    // Add other relevant ES index properties if needed
                    fieldIndex.setProperties(indexProps);
                    indexes.add(fieldIndex);
                }
            }
        }
        tableStructure.setIndexes(indexes);
    }
    
    private RestHighLevelClient getElasticsearchClient(String dataSourceName) {
        RestHighLevelClient client = elasticsearchClientMap.get(dataSourceName);
        if (client == null) {
            // Log and throw or handle as per application's error strategy
            logger.error("Elasticsearch client instance not found for dataSourceName: '{}'", dataSourceName);
            throw new IllegalStateException("Elasticsearch client not configured for: " + dataSourceName);
        }
        return client;
    }
} 