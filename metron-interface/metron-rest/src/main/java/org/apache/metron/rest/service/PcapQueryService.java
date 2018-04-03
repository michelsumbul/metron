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
package org.apache.metron.rest.service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.metron.rest.RestException;
import org.apache.metron.rest.util.PcapQueryResult;

/**
 *
 * @author msumbul
 */
public interface PcapQueryService {
    

    List<PcapQueryResult>  QueryFilterUtility(String sourceIP,
            String destinationIP,
            int sourcePort,
            int destinationPort,
            int protocol,
            long st,
            long et,
            String dateformat,
            Path hdfsPathPcap,
            String packet_filter,
            Boolean include_reverse);
    
    

    
    
}
