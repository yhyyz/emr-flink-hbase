/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aws.analytics;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
public class DataGen2Hbase {
	public static void main(String[] args) throws Exception {

		ParameterTool parameter = ParameterTool.fromArgs(args);
		String  hbase_zk = parameter.get("hbase_zk");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.enableCheckpointing(10000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStateBackend(new EmbeddedRocksDBStateBackend());
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 每秒生产10条数据
		String dataGenTableSQL="CREATE TABLE source_table (\n" +
				" row_key STRING,\n" +
				" user_id STRING,\n" +
				" product_id STRING\n" +
				") WITH (\n" +
				"  'rows-per-second' = '10',\n" +
				"  'connector' = 'datagen'\n" +
				")";
        // hbase connector 其它相关参数如下链接
		// https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/hbase/#connector-options
		String hbaseTableSQL=String.format("CREATE TABLE hbase_datagen_table (\n" +
				" row_key STRING,\n" +
				" cf ROW<user_id STRING,product_id STRING>,\n" +
				" PRIMARY KEY (row_key) NOT ENFORCED\n" +
				") WITH (\n" +
				" 'connector' = 'hbase-2.2',\n" +
				" 'table-name' = 'hbase_datagen_table',\n" +
				" 'zookeeper.quorum' = '%s'\n" +
				");",hbase_zk);

		tableEnv.executeSql(dataGenTableSQL);
		tableEnv.executeSql(hbaseTableSQL);
		String sinkHBaseSQL="insert into hbase_datagen_table select row_key,ROW(user_id,product_id) from source_table";
		StreamStatementSet sss= tableEnv.createStatementSet();
		sss.addInsertSql(sinkHBaseSQL).execute();
	}
}
