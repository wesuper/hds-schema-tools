package org.immortal.hydra.hdstbcomp.extractor;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.immortal.hydra.hdstbcomp.config.DataSourceConfig;
import org.immortal.hydra.hdstbcomp.model.ColumnStructure;
import org.immortal.hydra.hdstbcomp.model.IndexStructure;
import org.immortal.hydra.hdstbcomp.model.TableStructure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
@Component
public class ElasticsearchTableStructureExtractor implements TableStructureExtractor {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTableStructureExtractor.class);
    
    private static final String TYPE = "elasticsearch";
    
    @Autowired
    private Map<String, RestHighLevelClient> elasticsearchClientMap;
    
    @Override
    public TableStructure extractTableStructure(DataSourceConfig.TableConfig tableConfig) throws Exception {
        String dataSourceName = tableConfig.getDataSourceName();
        String indexName = tableConfig.getTableName();
        
        logger.info("Extracting structure for Elasticsearch index: {} from datasource: {}", indexName, dataSourceName);
        
        RestHighLevelClient client = getElasticsearchClient(dataSourceName);
        if (client == null) {
            throw new IllegalArgumentException("Elasticsearch client not found: " + dataSourceName);
        }
        
        TableStructure tableStructure = new TableStructure();
        tableStructure.setTableName(indexName);
        tableStructure.setSourceType(TYPE);
        
        try {
            // 获取索引信息
            GetIndexRequest request = new GetIndexRequest().indices(indexName);
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
            
            // 获取索引设置
            Settings indexSettings = response.getSettings().get(indexName);
            extractIndexSettings(tableStructure, indexSettings);
            
            // 获取映射信息
            ImmutableOpenMap<String, MappingMetadata> mappings = response.getMappings().get(indexName);
            if (mappings != null && !mappings.isEmpty()) {
                // 在ES 7.x以后，类型可能为_doc或者被弃用
                String mappingType = "_doc";
                if (mappings.containsKey(mappingType)) {
                    MappingMetadata mappingMetaData = mappings.get(mappingType);
                    extractMappingMetadata(tableStructure, mappingMetaData);
                } else {
                    // 如果没有_doc类型，获取第一个可用的映射
                    for (Object key : mappings.keys().toArray()) {
                        String type = key.toString();
                        MappingMetadata mappingMetaData = mappings.get(type);
                        extractMappingMetadata(tableStructure, mappingMetaData);
                        break;
                    }
                }
            }
            
            return tableStructure;
        } catch (Exception e) {
            logger.error("Failed to extract Elasticsearch index structure for {}: {}", indexName, e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public String getSupportedType() {
        return TYPE;
    }
    
    /**
     * 提取索引设置
     * 
     * @param tableStructure 表结构
     * @param settings 索引设置
     */
    private void extractIndexSettings(TableStructure tableStructure, Settings settings) {
        Map<String, Object> properties = tableStructure.getProperties();
        
        // 存储关键设置
        properties.put("number_of_shards", settings.get("index.number_of_shards"));
        properties.put("number_of_replicas", settings.get("index.number_of_replicas"));
        properties.put("creation_date", settings.get("index.creation_date"));
        properties.put("uuid", settings.get("index.uuid"));
        properties.put("provided_name", settings.get("index.provided_name"));
        
        // 添加分析器信息
        if (settings.get("index.analysis") != null) {
            properties.put("has_custom_analyzers", true);
        }
    }
    
    /**
     * 提取映射元数据
     * 
     * @param tableStructure 表结构
     * @param mappingMetaData 映射元数据
     */
    @SuppressWarnings("unchecked")
    private void extractMappingMetadata(TableStructure tableStructure, MappingMetadata mappingMetaData) {
        try {
            Map<String, Object> mappingMap = mappingMetaData.sourceAsMap();
            
            // 提取字段映射
            Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
            if (properties != null) {
                int position = 1;
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    String fieldName = entry.getKey();
                    Map<String, Object> fieldProperties = (Map<String, Object>) entry.getValue();
                    
                    ColumnStructure column = new ColumnStructure();
                    column.setColumnName(fieldName);
                    column.setOrdinalPosition(position++);
                    
                    extractFieldProperties(column, fieldProperties);
                    
                    tableStructure.getColumns().add(column);
                }
            }
            
            // 提取或创建索引信息
            extractIndexInformation(tableStructure, mappingMap);
            
        } catch (Exception e) {
            logger.error("Error extracting Elasticsearch mapping metadata: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 提取字段属性
     * 
     * @param column 列结构
     * @param fieldProperties 字段属性
     */
    private void extractFieldProperties(ColumnStructure column, Map<String, Object> fieldProperties) {
        String type = (String) fieldProperties.get("type");
        column.setDataType(type);
        column.setColumnType(type);  // ES中没有详细的列类型，使用基本类型
        
        // ES中几乎所有字段默认都是可空的
        column.setNullable(true);
        
        // 复制原始属性到列属性中
        Map<String, Object> properties = new HashMap<>(fieldProperties);
        column.setProperties(properties);
        
        // 根据类型提取特定属性
        if ("text".equals(type)) {
            // 文本类型，可能有分析器
            column.setProperties(properties);
            if (fieldProperties.containsKey("analyzer")) {
                properties.put("analyzer", fieldProperties.get("analyzer"));
            }
        } else if ("keyword".equals(type)) {
            // 关键词类型
            if (fieldProperties.containsKey("ignore_above")) {
                Integer ignoreAbove = (Integer) fieldProperties.get("ignore_above");
                column.setLength(ignoreAbove);
            }
        } else if (type != null && (type.startsWith("float") || type.startsWith("double") || 
                type.equals("half_float") || type.equals("scaled_float"))) {
            // 浮点类型
            if (fieldProperties.containsKey("scaling_factor")) {
                properties.put("scaling_factor", fieldProperties.get("scaling_factor"));
            }
        }
    }
    
    /**
     * 提取索引信息
     * 
     * @param tableStructure 表结构
     * @param mappingMap 映射映射
     */
    @SuppressWarnings("unchecked")
    private void extractIndexInformation(TableStructure tableStructure, Map<String, Object> mappingMap) {
        List<IndexStructure> indexes = new ArrayList<>();
        
        // 检查是否有显式的_id字段
        if (mappingMap.containsKey("_id")) {
            IndexStructure primaryIndex = new IndexStructure();
            primaryIndex.setIndexName("_id");
            primaryIndex.setIndexType("PRIMARY");
            primaryIndex.setPrimary(true);
            primaryIndex.setUnique(true);
            
            IndexStructure.IndexColumnStructure column = new IndexStructure.IndexColumnStructure();
            column.setColumnName("_id");
            column.setPosition(1);
            
            List<IndexStructure.IndexColumnStructure> columns = new ArrayList<>();
            columns.add(column);
            primaryIndex.setColumns(columns);
            
            indexes.add(primaryIndex);
        }
        
        // 检查其他字段上的索引
        Map<String, Object> properties = (Map<String, Object>) mappingMap.get("properties");
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String fieldName = entry.getKey();
                Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
                
                // 检查索引标志
                Object indexFlag = fieldProps.get("index");
                boolean isIndexed = indexFlag == null || Boolean.TRUE.equals(indexFlag) || "true".equals(indexFlag);
                
                if (isIndexed) {
                    // 创建索引结构
                    IndexStructure fieldIndex = new IndexStructure();
                    fieldIndex.setIndexName(fieldName + "_idx");
                    fieldIndex.setIndexType("NORMAL");
                    fieldIndex.setPrimary(false);
                    fieldIndex.setUnique(false);
                    
                    IndexStructure.IndexColumnStructure column = new IndexStructure.IndexColumnStructure();
                    column.setColumnName(fieldName);
                    column.setPosition(1);
                    
                    List<IndexStructure.IndexColumnStructure> columns = new ArrayList<>();
                    columns.add(column);
                    fieldIndex.setColumns(columns);
                    
                    // 保存索引属性
                    Map<String, Object> indexProps = new HashMap<>();
                    if (fieldProps.containsKey("analyzer")) {
                        indexProps.put("analyzer", fieldProps.get("analyzer"));
                    }
                    if (fieldProps.containsKey("search_analyzer")) {
                        indexProps.put("search_analyzer", fieldProps.get("search_analyzer"));
                    }
                    fieldIndex.setProperties(indexProps);
                    
                    indexes.add(fieldIndex);
                }
            }
        }
        
        tableStructure.setIndexes(indexes);
    }
    
    /**
     * 获取Elasticsearch客户端
     * 
     * @param dataSourceName 数据源名称
     * @return Elasticsearch客户端
     */
    private RestHighLevelClient getElasticsearchClient(String dataSourceName) {
        RestHighLevelClient client = elasticsearchClientMap.get(dataSourceName);
        if (client == null) {
            logger.error("Elasticsearch client not found: {}", dataSourceName);
        }
        return client;
    }
} 