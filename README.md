# dla-spark-demo
 
1. pleaes replace the credential in /src/main/template/application.conf
2. copy and overwrite /src/main/template/application.conf to  /src/main/resources/application.conf


# Run DLA Spark with cmd line tool (https://www.alibabacloud.com/help/zh/doc-detail/181846.htm)

1. 您可以通过wget的方式进行下载Spark-Submit命令行工具。
wget https://dla003.oss-cn-hangzhou.aliyuncs.com/dla_spark_toolkit_1/dla-spark-toolkit.tar.gz

2. unzip dla-spark-toolkit.tar.gz to {targetpath}

3. 修改 /src/main/resources/spark-defaults.conf 文档， 然后覆盖 {targetpath}\conf\spark-defaults.conf

