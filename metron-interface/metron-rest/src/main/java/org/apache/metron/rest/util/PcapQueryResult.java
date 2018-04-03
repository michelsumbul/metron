/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.rest.util;

import java.sql.Timestamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author msumbul
 */
public class PcapQueryResult {

    private String sourceIP;
    private String destinationIP;
    private int sourcePort;
    private int destinationPort;
    private String protocol;
    private long ts;
    private Path hdfsPathPcap;
    private Configuration configuration;

    public PcapQueryResult(String sourcIP,
            String destinationIP,
            int sourcePort,
            int destinationPort,
            String protocol,
            long ts,
            Path hdfsPathPcap) {

        this.sourceIP = sourcIP;
        this.destinationIP = destinationIP;
        this.sourcePort = sourcePort;
        this.destinationPort = destinationPort;
        this.protocol = protocol;
        this.ts = ts;
        this.hdfsPathPcap = hdfsPathPcap;

    }

    public PcapQueryResult(Path hdfsPathFile, Configuration configuration) {
        this.hdfsPathPcap = hdfsPathFile;
        this.configuration = configuration;
    }

}
