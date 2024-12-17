echo.
echo Python Version:
python --version
pause

echo.
echo Checking Spark:
spark-submit --version
pause

echo.
echo Checking Hadoop:
hadoop version
pause

echo.
echo Environment Variables:
echo JAVA_HOME: %JAVA_HOME%
echo PYTHON_HOME: %PYTHON_HOME%
echo SPARK_HOME: %SPARK_HOME%
echo HADOOP_HOME: %HADOOP_HOME%
pause
