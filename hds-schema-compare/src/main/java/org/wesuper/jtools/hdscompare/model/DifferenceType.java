package org.wesuper.jtools.hdscompare.model;

/**
 * 差异类型枚举
 */
public enum DifferenceType {
    // 列相关差异
    COLUMN_MISSING,           // 列缺失
    COLUMN_TYPE_DIFFERENT,    // 列类型不同
    COLUMN_PROPERTY_DIFFERENT,// 列属性不同
    
    // 索引相关差异
    INDEX_MISSING,           // 索引缺失
    INDEX_STRUCTURE_DIFFERENT,// 索引结构不同
    
    // 表相关差异
    TABLE_PROPERTY_DIFFERENT // 表属性不同
} 