/**
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
package org.apache.metron.rest.config;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Bean;






/**
 * utility class for this module which loads commons configuration to fetch
 * properties from underlying resources to communicate with HDFS.
 * 
 */
@org.springframework.context.annotation.Configuration
public class PcapConfig {

	private static Configuration propConfiguration = null;


	/**
	 * Loads configuration resources 
	 * @return Configuration
	 */
        @Bean
	public synchronized static Configuration getConfiguration() {
		if(propConfiguration == null){
			propConfiguration = new Configuration();
		}
		return propConfiguration;
	}

	public static String getPcapOutputPath() {
		return getConfiguration().get("pcap.output.path");
	}

	public static void setPcapOutputPath(String path) {
		getConfiguration().set("pcap.output.path", path);
	}

	public static String getTempQueryOutputPath() {
		return getConfiguration().get("temp.query.output.path");
	}
	public static void setTempQueryOutputPath(String path) {
		getConfiguration().set("temp.query.output.path", path);
	}



}
