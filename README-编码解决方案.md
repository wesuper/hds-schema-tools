# Maven Windows 10 控制台乱码解决方案

## 问题描述
在 Windows 10 系统中使用 Maven 编译时，控制台可能出现中文乱码问题。

## 解决方案

### 1. 项目配置 (已完成)
在 `pom.xml` 中已添加以下配置：

```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
    <maven.compiler.encoding>UTF-8</maven.compiler.encoding>
    <file.encoding>UTF-8</file.encoding>
</properties>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <configuration>
                <encoding>UTF-8</encoding>
                <compilerArgs>
                    <arg>-Dfile.encoding=UTF-8</arg>
                </compilerArgs>
            </configuration>
        </plugin>
    </plugins>
</build>
```

### 2. JVM 配置 (已完成)
在 `.mvn/jvm.config` 文件中已添加：
```
-Dfile.encoding=UTF-8
-Dconsole.encoding=UTF-8
-Duser.language=zh
-Duser.country=CN
```

### 3. 使用脚本运行 Maven (推荐)

#### PowerShell 用户
使用 `mvn-utf8.ps1` 脚本：
```powershell
.\mvn-utf8.ps1 clean compile
.\mvn-utf8.ps1 package
```

#### CMD 用户
使用 `mvn-utf8.bat` 脚本：
```cmd
mvn-utf8.bat clean compile
mvn-utf8.bat package
```

### 4. 手动设置环境变量
如果不想使用脚本，可以手动设置：

#### PowerShell
```powershell
chcp 65001
$env:JAVA_TOOL_OPTIONS = "-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8"
$env:MAVEN_OPTS = "-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8"
mvn clean compile
```

#### CMD
```cmd
chcp 65001
set JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8
set MAVEN_OPTS=-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8
mvn clean compile
```

### 5. 永久解决方案
将以下环境变量添加到系统环境变量中：
- `JAVA_TOOL_OPTIONS`: `-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8`
- `MAVEN_OPTS`: `-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8`

## 验证
运行以下命令验证编码设置：
```
.\mvn-utf8.ps1 --version
```

应该看到输出中包含：`platform encoding: UTF-8` 