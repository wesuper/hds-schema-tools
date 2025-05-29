package org.wesuper.jtools.hdscompare.extractor;

import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.model.TableStructure;

/**
 * 表结构提取器接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface TableStructureExtractor {

    /**
     * 提取表结构
     *
     * @param dataSourceConfig 数据源配置
     * @param tableName 表名
     * @return 表结构
     * @throws Exception 提取失败时抛出异常
     */
    TableStructure extractTableStructure(DataSourceCompareConfig.DataSourceConfig dataSourceConfig, String tableName) throws Exception;
    
    /**
     * 获取支持的数据源类型
     *
     * @return 数据源类型
     */
    String getSupportedType();
} 