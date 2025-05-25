# 设置控制台编码为 UTF-8
chcp 65001 | Out-Null

# 设置环境变量
$env:JAVA_TOOL_OPTIONS = "-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8"
$env:MAVEN_OPTS = "-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8"

# 运行 Maven 命令
mvn @args 