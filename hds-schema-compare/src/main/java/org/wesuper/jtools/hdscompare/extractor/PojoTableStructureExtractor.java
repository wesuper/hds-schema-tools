package org.wesuper.jtools.hdscompare.extractor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wesuper.jtools.hdscompare.config.DataSourceCompareConfig;
import org.wesuper.jtools.hdscompare.constants.DatabaseType;
import org.wesuper.jtools.hdscompare.model.ColumnStructure;
import org.wesuper.jtools.hdscompare.model.TableStructure;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * POJO表结构提取器实现
 * 用于从Java实体类中提取结构信息
 * 
 * @author vincentruan
 * @version 1.0.0
 */
public class PojoTableStructureExtractor implements TableStructureExtractor {

    private static final Logger logger = LoggerFactory.getLogger(PojoTableStructureExtractor.class);
    
    private static final String TYPE = DatabaseType.POJO;
    private final Map<String, TableStructure> pojoCache = new ConcurrentHashMap<>(); // Cache for POJO structures
    
    // 使用Guava的Multimap优化Java类型到数据库类型的映射
    private static final Multimap<String, String> JAVA_TO_MYSQL_TYPE_MAPPING = ArrayListMultimap.create();
    private static final Multimap<String, String> JAVA_TO_ES_TYPE_MAPPING = ArrayListMultimap.create();
    
    static {
        // Java类型到MySQL类型的映射
        JAVA_TO_MYSQL_TYPE_MAPPING.put(long.class.getName(), "bigint");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Long.class.getName(), "bigint");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(int.class.getName(), "int");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Integer.class.getName(), "int");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(short.class.getName(), "smallint");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Short.class.getName(), "smallint");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(byte.class.getName(), "tinyint");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Byte.class.getName(), "tinyint");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(float.class.getName(), "float");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Float.class.getName(), "float");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(double.class.getName(), "double");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Double.class.getName(), "double");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(BigDecimal.class.getName(), "decimal");
        
        JAVA_TO_MYSQL_TYPE_MAPPING.put(String.class.getName(), "varchar");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(String.class.getName(), "text"); 
        JAVA_TO_MYSQL_TYPE_MAPPING.put(String.class.getName(), "enum"); 
        JAVA_TO_MYSQL_TYPE_MAPPING.put(char.class.getName(), "char");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Character.class.getName(), "char");
        
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Date.class.getName(), "date");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Date.class.getName(), "datetime");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Date.class.getName(), "timestamp");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(LocalDate.class.getName(), "date");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(LocalDateTime.class.getName(), "datetime");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(LocalTime.class.getName(), "time");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Timestamp.class.getName(), "timestamp");
        
        JAVA_TO_MYSQL_TYPE_MAPPING.put(boolean.class.getName(), "boolean");
        JAVA_TO_MYSQL_TYPE_MAPPING.put(Boolean.class.getName(), "boolean");

        // Java类型到ES类型的映射
        JAVA_TO_ES_TYPE_MAPPING.put(long.class.getName(), "long");
        JAVA_TO_ES_TYPE_MAPPING.put(Long.class.getName(), "long");
        JAVA_TO_ES_TYPE_MAPPING.put(int.class.getName(), "integer");
        JAVA_TO_ES_TYPE_MAPPING.put(Integer.class.getName(), "integer");
        JAVA_TO_ES_TYPE_MAPPING.put(short.class.getName(), "short");
        JAVA_TO_ES_TYPE_MAPPING.put(Short.class.getName(), "short");
        JAVA_TO_ES_TYPE_MAPPING.put(byte.class.getName(), "byte");
        JAVA_TO_ES_TYPE_MAPPING.put(Byte.class.getName(), "byte");
        JAVA_TO_ES_TYPE_MAPPING.put(float.class.getName(), "float");
        JAVA_TO_ES_TYPE_MAPPING.put(Float.class.getName(), "float");
        JAVA_TO_ES_TYPE_MAPPING.put(double.class.getName(), "double");
        JAVA_TO_ES_TYPE_MAPPING.put(Double.class.getName(), "double");
        JAVA_TO_ES_TYPE_MAPPING.put(BigDecimal.class.getName(), "scaled_float");
        JAVA_TO_ES_TYPE_MAPPING.put(BigDecimal.class.getName(), "double");
        
        JAVA_TO_ES_TYPE_MAPPING.put(String.class.getName(), "keyword");
        JAVA_TO_ES_TYPE_MAPPING.put(String.class.getName(), "text");
        JAVA_TO_ES_TYPE_MAPPING.put(char.class.getName(), "keyword");
        JAVA_TO_ES_TYPE_MAPPING.put(Character.class.getName(), "keyword");
        
        JAVA_TO_ES_TYPE_MAPPING.put(Date.class.getName(), "date");
        JAVA_TO_ES_TYPE_MAPPING.put(LocalDate.class.getName(), "date");
        JAVA_TO_ES_TYPE_MAPPING.put(LocalDateTime.class.getName(), "date");
        JAVA_TO_ES_TYPE_MAPPING.put(LocalTime.class.getName(), "date"); 
        JAVA_TO_ES_TYPE_MAPPING.put(Timestamp.class.getName(), "date");
        
        JAVA_TO_ES_TYPE_MAPPING.put(boolean.class.getName(), "boolean");
        JAVA_TO_ES_TYPE_MAPPING.put(Boolean.class.getName(), "boolean");
    }
    
    @Override
    public TableStructure extractTableStructure(DataSourceCompareConfig.DataSourceConfig dataSourceConfig, String className) throws Exception {
        // Check cache first
        TableStructure cachedStructure = pojoCache.get(className);
        if (cachedStructure != null) {
            logger.info("Returning cached structure for POJO class: {}", className);
            // Return a deep copy to prevent modification of cached object if necessary, 
            // or ensure TableStructure and its components are immutable or defensively copied.
            // For now, returning direct reference assuming it's handled or not an issue.
            return cachedStructure; 
        }

        logger.info("Extracting structure for POJO class: {}", className);
        
        Class<?> clazz = Class.forName(className);
        TableStructure tableStructure = new TableStructure();
        tableStructure.setTableName(clazz.getSimpleName());
        tableStructure.setSourceType(TYPE);
        
        // 提取类注释作为表注释
        tableStructure.setTableComment(getClassComment(clazz));
        
        // 提取字段信息
        List<ColumnStructure> columns = extractColumns(clazz);
        tableStructure.setColumns(columns);

        // Log the state of typeMappings for all columns before returning from extractor
        if (logger.isDebugEnabled()) {
            logger.debug("Final TypeMappings in TableStructure for POJO class '{}' before returning:", className);
            for (ColumnStructure col : columns) {
                logger.debug("  Column '{}':", col.getColumnName());
                if (col.getTypeMappings() != null && !col.getTypeMappings().isEmpty()) {
                    for (ColumnStructure.TypeMapping tm : col.getTypeMappings()) {
                        logger.debug("    - TargetDB: '{}', MappedTypes: {}", tm.getTargetType(), tm.getColumnTypes());
                    }
                } else {
                    logger.debug("    - No TypeMappings present.");
                }
            }
        }
        
        // Store in cache before returning
        pojoCache.put(className, tableStructure);
        return tableStructure;
    }
    
    @Override
    public String getSupportedType() {
        return TYPE;
    }
    
    /**
     * 获取类的注释
     */
    private String getClassComment(Class<?> clazz) {
        // 这里可以扩展支持更多的注释注解，如javadoc等
        return "";
    }
    
    /**
     * 提取类的字段信息
     */
    private List<ColumnStructure> extractColumns(Class<?> clazz) {
        logger.debug("PojoTableStructureExtractor.extractColumns called for class: {}", clazz.getName());
        logger.debug("Static JAVA_TO_MYSQL_TYPE_MAPPING size: {}", JAVA_TO_MYSQL_TYPE_MAPPING.size());
        logger.debug("Static JAVA_TO_MYSQL_TYPE_MAPPING content: {}", JAVA_TO_MYSQL_TYPE_MAPPING);
        logger.debug("Static JAVA_TO_ES_TYPE_MAPPING size: {}", JAVA_TO_ES_TYPE_MAPPING.size());
        logger.debug("Static JAVA_TO_ES_TYPE_MAPPING content: {}", JAVA_TO_ES_TYPE_MAPPING);

        List<ColumnStructure> columns = new ArrayList<>();
        int ordinalPosition = 1;
        
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isSynthetic()) {
                continue;
            }

            ColumnStructure column = new ColumnStructure();
            String fieldName = getFieldName(field);
            column.setColumnName(fieldName);
            
            String javaTypeFullName = field.getType().getName(); 
            String javaTypeSimpleName = field.getType().getSimpleName();
            column.setDataType(javaTypeSimpleName); 
            column.setOrdinalPosition(ordinalPosition++);

            logger.debug("Extracting POJO column: '{}', Field Java Type (getName()): '{}', SimpleName: '{}'", 
                         fieldName, javaTypeFullName, javaTypeSimpleName);
            logger.debug("Attempting to get MySQL mappings for javaType '{}': Result: {}", javaTypeFullName, JAVA_TO_MYSQL_TYPE_MAPPING.get(javaTypeFullName));
            logger.debug("Attempting to get ES mappings for javaType '{}': Result: {}", javaTypeFullName, JAVA_TO_ES_TYPE_MAPPING.get(javaTypeFullName));
            
            for (String mysqlType : JAVA_TO_MYSQL_TYPE_MAPPING.get(javaTypeFullName)) {
                logger.debug("Adding MySQL mapping for field '{}': JavaType '{}' -> MySQLType '{}'", fieldName, javaTypeFullName, mysqlType);
                column.addTypeMapping(DatabaseType.MYSQL, mysqlType);
                column.addTypeMapping(DatabaseType.TIDB, mysqlType);
            }
            
            for (String esType : JAVA_TO_ES_TYPE_MAPPING.get(javaTypeFullName)) {
                logger.debug("Adding ES mapping for field '{}': JavaType '{}' -> ESType '{}'", fieldName, javaTypeFullName, esType);
                column.addTypeMapping(DatabaseType.ELASTICSEARCH, esType);
            }
            
            column.setNullable(true); 
            Map<String, Object> properties = new HashMap<>();
            properties.put("javaType", javaTypeFullName); 
            column.setProperties(properties);
            
            columns.add(column);
        }
        
        return columns;
    }
    
    /**
     * 获取字段名，优先使用JsonProperty注解的值
     */
    private String getFieldName(Field field) {
        JsonProperty jsonProperty = field.getAnnotation(JsonProperty.class);
        if (jsonProperty != null && !jsonProperty.value().isEmpty()) {
            return jsonProperty.value();
        }
        return field.getName();
    }
    
    /**
     * 获取字段注释
     */
    private String getFieldComment(Field field) {
        // 这里可以扩展支持更多的注释注解
        return "";
    }
} 