package org.immortal.hydra.hdstbcomp.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * 表结构提取器工厂类，根据数据源类型获取对应的表结构提取器
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Component
public class TableStructureExtractorFactory {

    private static final Logger logger = LoggerFactory.getLogger(TableStructureExtractorFactory.class);
    
    private final Map<String, TableStructureExtractor> extractorMap = new HashMap<>();

    /**
     * 构造函数，自动注入所有实现了TableStructureExtractor接口的提取器
     *
     * @param extractors 所有的表结构提取器列表
     */
    @Autowired
    public TableStructureExtractorFactory(List<TableStructureExtractor> extractors) {
        for (TableStructureExtractor extractor : extractors) {
            String type = extractor.getSupportedType();
            extractorMap.put(type.toLowerCase(java.util.Locale.ROOT), extractor);
            logger.info("Registered table structure extractor for type: {}", type);
        }
    }

    /**
     * 根据数据源类型获取对应的表结构提取器
     *
     * @param sourceType 数据源类型
     * @return 对应的表结构提取器，如果未找到则返回null
     */
    public TableStructureExtractor getExtractor(String sourceType) {
        if (sourceType == null) {
            return null;
        }
        
        TableStructureExtractor extractor = extractorMap.get(sourceType.toLowerCase(java.util.Locale.ROOT));
        if (extractor == null) {
            logger.warn("No table structure extractor found for type: {}", sourceType);
        }
        
        return extractor;
    }
    
    /**
     * 判断是否支持指定类型的提取器
     *
     * @param sourceType 数据源类型
     * @return 是否支持
     */
    public boolean supportsType(String sourceType) {
        return sourceType != null && extractorMap.containsKey(sourceType.toLowerCase(java.util.Locale.ROOT));
    }
    
    /**
     * 获取所有支持的数据源类型
     *
     * @return 支持的数据源类型列表
     */
    public List<String> getSupportedTypes() {
        return new ArrayList<>(extractorMap.keySet());
    }
} 