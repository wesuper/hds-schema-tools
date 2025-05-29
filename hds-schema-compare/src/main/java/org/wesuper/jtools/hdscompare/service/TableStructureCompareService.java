package org.wesuper.jtools.hdscompare.service;

import java.util.List;

import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.model.CompareResult;
import org.wesuper.jtools.hdscompare.model.TableStructure;

/**
 * 表结构比对服务接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface TableStructureCompareService {

    /**
     * 比对两个表的结构
     *
     * @param sourceTable 源表结构
     * @param targetTable 目标表结构
     * @param config      比对配置
     * @return 比对结果
     */
    CompareResult compareTableStructures(TableStructure sourceTable, TableStructure targetTable, 
                                        DataSourceCompareConfig.CompareConfig config);
    
    /**
     * 比对所有配置的表
     * 
     * @return 比对结果列表
     */
    List<CompareResult> compareAllConfiguredTables();
    
    /**
     * 根据配置名称比对表
     *
     * @param name 配置名称
     * @return 比对结果
     */
    CompareResult compareTablesByName(String name);
    
    /**
     * 获取表结构
     *
     * @param dataSourceConfig 数据源配置
     * @param tableName        表名
     * @return 表结构
     * @throws Exception 获取表结构时可能抛出的异常
     */
    TableStructure getTableStructure(DataSourceCompareConfig.DataSourceConfig dataSourceConfig, String tableName) throws Exception;
} 