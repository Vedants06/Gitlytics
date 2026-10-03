#!/usr/bin/env python3
import os
import xml.etree.ElementTree as ET

hadoop_home = os.environ.get("HADOOP_HOME", os.path.expanduser("~/bigdata/hadoop"))
path = os.path.join(hadoop_home, "etc/hadoop/mapred-site.xml")

xml_content = """<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>localhost:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>localhost:19888</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>/home/vedant/bigdata/hadoop/share/hadoop/mapreduce/*:/home/vedant/bigdata/hadoop/share/hadoop/mapreduce/lib/*</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=/home/vedant/bigdata/hadoop</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=/home/vedant/bigdata/hadoop</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=/home/vedant/bigdata/hadoop</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.resource.mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>1024</value>
  </property>
</configuration>
"""

with open(path, "w") as f:
    f.write(xml_content.strip() + "\n")

# Validate XML syntax
tree = ET.parse(path)
print(f"SUCCESS: {path} written and XML syntax is 100% valid.")