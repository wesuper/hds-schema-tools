-- MySQL模拟表
CREATE TABLE IF NOT EXISTS mysql_table (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(500),
    status CHAR(1) NOT NULL DEFAULT '1',
    amount DECIMAL(10,2),
    data_type VARCHAR(50),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- 创建索引
CREATE UNIQUE INDEX mysql_table_uk_name ON mysql_table (name);
CREATE INDEX mysql_table_idx_status ON mysql_table (status);

-- 注释 (H2使用COMMENT ON语句)
COMMENT ON TABLE mysql_table IS '模拟MySQL表';
COMMENT ON COLUMN mysql_table.id IS '自增主键';
COMMENT ON COLUMN mysql_table.name IS '名称';
COMMENT ON COLUMN mysql_table.description IS '描述信息';
COMMENT ON COLUMN mysql_table.status IS '状态：1-启用 0-禁用';
COMMENT ON COLUMN mysql_table.amount IS '金额';
COMMENT ON COLUMN mysql_table.data_type IS '数据类型';
COMMENT ON COLUMN mysql_table.create_time IS '创建时间';
COMMENT ON COLUMN mysql_table.update_time IS '更新时间';

-- TiDB模拟表 (有一些细微的差异)
CREATE TABLE IF NOT EXISTS tidb_table (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(255),  -- 长度不同
    status CHAR(1) NOT NULL DEFAULT '1',
    amount DECIMAL(12,4),  -- 精度不同
    data_type VARCHAR(50),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extra_column VARCHAR(100), -- 额外列
    PRIMARY KEY (id)
);

-- 创建索引
CREATE UNIQUE INDEX tidb_table_uk_name ON tidb_table (name);
CREATE INDEX tidb_table_idx_status_type ON tidb_table (status, data_type);

-- 注释
COMMENT ON TABLE tidb_table IS '模拟TiDB表';
COMMENT ON COLUMN tidb_table.id IS '自增主键';
COMMENT ON COLUMN tidb_table.name IS '名称';
COMMENT ON COLUMN tidb_table.description IS '描述信息(简短)';  -- 注释不同
COMMENT ON COLUMN tidb_table.status IS '状态标识';  -- 注释不同
COMMENT ON COLUMN tidb_table.amount IS '金额';
COMMENT ON COLUMN tidb_table.data_type IS '数据类型';
COMMENT ON COLUMN tidb_table.create_time IS '创建时间';
COMMENT ON COLUMN tidb_table.update_time IS '更新时间';
COMMENT ON COLUMN tidb_table.extra_column IS '仅在TiDB中存在的列'; 