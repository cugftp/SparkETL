# Spark ETL
使用spark大数据平台进行数据抽取、清洗和转换等工作。

### 整体结构
1. cmd包为程序的入口，程序提供两个入口，在调用spark-submit的时候可以选择使用SQL数据库或者是kafka作为数据源
2. config包主要处理配置文件的读取和生成对应的配置对象，config包又有3个子包分别处理数据源、过滤条件和目标数据库的配置
3. filter包主要实现对数据的清洗工作，需要传入清洗操作的配置对象
4. writer包主要实现按配置创建数据表和将结果写入数据表的操作

### 程序启动指令参考
需要给出3个配置文件的绝对路径作为参数传入

``` ./spark-submit --master "local" --name "etl-test-01" --class "cc.lfwzc.cmd.SparkSQLETL" sparkETL.jar "/xxx/srcconfig.json" "/xxx/filterconfig.json" "/xxx/dstconfig.json" ```

### 数据处理要求
数据清洗：
1. 空值剔除，重复的数据能够剔除
2. 不符合值域范围的字段能够剔除（某字段值过大、过小）， 错误格式的数据能够剔除（日期格式不正确）
3. 根据配置文件，可以剔除某些不需要的字段

数据转换：
1. 日期格式转换（时间戳和日期（如YYYY-mm-dd，）格式相互转化，日期格式之间相互转化（dddd-mm-yyyy转YYYY-mm-dd）
2. 数值类型字段单位转换（元转万元， 秒转小时等）
3. 字符串类型字段转换（如更改某字符串前缀'xx001'转为'yy001'）

### TODO
1. 使用SparkSQL API从数据库获取数据
2. 编写过滤器，实现清洗操作
3. 编写ORACLE等常用数据库的写入操作

