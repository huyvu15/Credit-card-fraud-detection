export HBASE_HOME=/hbase
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/sqoop/lib/*

sqoop import \
  --connect "jdbc:mysql://upgradawsrds1.cyajelc9bmnf.us-east-1.rds.amazonaws.com/cred_financials_data" \
  --username upgraduser \
  --password upgraduser \
  --table card_member \
  --hbase-table card_member \
  --column-family cf \
  --hbase-row-key card_id \
  --num-mappers 1 \
  --hbase-zookeeper-quorum hbase:2181

sqoop import \
  --connect "jdbc:mysql://upgradawsrds1.cyajelc9bmnf.us-east-1.rds.amazonaws.com/cred_financials_data" \
  --username upgraduser \
  --password upgraduser \
  --table member_score \
  --hbase-table member_score \
  --column-family cf \
  --hbase-row-key member_id \
  --num-mappers 1 \
  --hbase-zookeeper-quorum hbase:2181