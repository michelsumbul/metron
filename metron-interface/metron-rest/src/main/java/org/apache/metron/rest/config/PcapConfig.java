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

/**
 * utility class for this module which loads commons configuration to fetch
 * properties from underlying resources to communicate with HDFS.
 *
 */
public class PcapConfig {

    private Configuration propConfiguration = null;

    /**
     * Loads configuration resources
     *
     * @return Configuration
     *
     */
    public PcapConfig() {
        propConfiguration = new Configuration();
        propConfiguration.set("pcap.output.path", "/tmp");
        propConfiguration.set("pcap.source.path", "/apps/metron/pcap");
        propConfiguration.addResource("/usr/hdp/current/hadoop-client/conf/mapred-site.xml");
        propConfiguration.addResource("/usr/hdp/current/hadoop-client/conf/yarn-site.xml");
        propConfiguration.addResource("/usr/hdp/current/hadoop-client/conf/core-site.xml");
        propConfiguration.addResource("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml");
         
    }

    public Configuration getConfiguration() {
        
        return this.propConfiguration;
    }

    public String getPcapOutputPath() {
        return getConfiguration().get("pcap.output.path");
    }

    public void setPcapOutputPath(String path) {
        getConfiguration().set("pcap.output.path", path);
    }

    public String getTempQueryOutputPath() {
        return getConfiguration().get("temp.query.output.path");
    }

    public void setTempQueryOutputPath(String path) {
        getConfiguration().set("temp.query.output.path", path);
    }

    public String getPcapSourcePath() {
        return getConfiguration().get("pcap.source.path");
    }

    public void setPcapSourcePath(String path) {
        getConfiguration().set("pcap.source.path", path);
    }

}
