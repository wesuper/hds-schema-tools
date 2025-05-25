@echo off
chcp 65001 > nul
set JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8
set MAVEN_OPTS=-Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8
mvn %* 