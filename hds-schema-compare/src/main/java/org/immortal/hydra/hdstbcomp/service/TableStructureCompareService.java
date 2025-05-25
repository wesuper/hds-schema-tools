package org.immortal.hydra.hdstbcomp.service;

import org.immortal.hydra.hdstbcomp.config.DataSourceConfig;
import org.immortal.hydra.hdstbcomp.model.CompareResult;
import org.immortal.hydra.hdstbcomp.model.TableStructure;

import java.util.List;

/**
 * 表结构比对服务接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface TableStructureCompareService {

    /**
     * 比对两个表结构之间的差异
     *
     * @param sourceTable 源表结构
     * @param targetTable 目标表结构
     * @param config 比对配置项
     * @return 比对结果
     */
    CompareResult compareTableStructures(TableStructure sourceTable, TableStructure targetTable, 
                                        DataSourceConfig.CompareConfig config);
    
    /**
     * 根据配置自动执行所有表结构比对
     * 
     * @return 所有表比对结果列表
     */
    List<CompareResult> compareAllConfiguredTables();
    
    /**
     * 根据名称执行特定表结构比对
     *
     * @param name 配置中的比对任务名称
     * @return 比对结果，如果未找到配置则返回null
     */
    CompareResult compareTablesByName(String name);
    
    /**
     * 获取表结构
     *
     * @param tableConfig 表配置
     * @return 表结构
     * @throws Exception 获取失败时抛出异常
     */
    TableStructure getTableStructure(DataSourceConfig.TableConfig tableConfig) throws Exception;
} 