package org.wesuper.jtools.hdscompare.extractor;

import org.wesuper.jtools.hdscompare.config.DataSourceConfig;
import org.wesuper.jtools.hdscompare.model.TableStructure;

/**
 * 表结构提取器接口
 * 定义统一的表结构提取方法，不同的数据源类型实现该接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface TableStructureExtractor {

    /**
     * 获取表的结构信息
     *
     * @param tableConfig 表配置信息
     * @return 统一的表结构描述对象
     * @throws Exception 提取过程中可能出现的异常
     */
    TableStructure extractTableStructure(DataSourceConfig.TableConfig tableConfig) throws Exception;
    
    /**
     * 获取当前提取器支持的数据源类型
     *
     * @return 数据源类型名称
     */
    String getSupportedType();
} 