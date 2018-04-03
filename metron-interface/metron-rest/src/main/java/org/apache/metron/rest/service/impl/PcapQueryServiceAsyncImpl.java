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
package org.apache.metron.rest.service.impl;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.metron.common.hadoop.SequenceFileIterable;
import org.apache.metron.common.utils.timestamp.TimestampConverters;
import org.apache.metron.pcap.filter.fixed.FixedPcapFilter;
import org.apache.metron.pcap.filter.query.QueryPcapFilter;
import org.apache.metron.pcap.mr.PcapJob;
import org.apache.metron.rest.RestException;
import org.apache.metron.rest.config.PcapConfig;
import org.apache.metron.rest.util.PcapsResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 *
 * @author msumbul We also reuse code from metron-api project
 */

public class PcapQueryServiceAsyncImpl {

    private static ThreadLocal<Configuration> CONFIGURATION = new ThreadLocal<Configuration>() {

        @Override
        protected Configuration initialValue() {
            Configuration config = PcapConfig.getConfiguration();
            
            return config;
        }
    };

    PcapJob queryUtil = new PcapJob();

    protected PcapJob getQueryUtil() {
        return queryUtil;
    }

    

    public ResponseEntity getPcapsByIdentifiers(Map<String, String> query, long startTime, long endTime, int numReducers) throws IOException, RestException {
        PcapsResponse response = new PcapsResponse();
        SequenceFileIterable results = null;
        try {
            if (startTime < 0) {
                startTime = 0L;
            }
            if (endTime < 0) {
                endTime = System.currentTimeMillis();
            }
            if (query == null) {
                return new ResponseEntity<>("Error: Query is null", HttpStatus.BAD_REQUEST);

            }
            //convert to nanoseconds since the epoch
            startTime = TimestampConverters.MILLISECONDS.toNanoseconds(startTime);
            endTime = TimestampConverters.MILLISECONDS.toNanoseconds(endTime);
            //LOGGER.debug("Query received: {}", query);

            results = getQueryUtil().query(new org.apache.hadoop.fs.Path(PcapConfig.getPcapOutputPath()),
                     new org.apache.hadoop.fs.Path(PcapConfig.getTempQueryOutputPath()),
                     startTime,
                     endTime,
                     numReducers,
                     query,
                     CONFIGURATION.get(),
                     FileSystem.get(CONFIGURATION.get()),
                     new FixedPcapFilter.Configurator()
            );
            response.setPcaps(results != null ? Lists.newArrayList(results) : null);
        } catch (Exception e) {
            throw new RestException(e);

        } finally {
            if (null != results) {
                results.cleanup();
            }
        }
        return new ResponseEntity<>(response.getPcaps(), HttpStatus.OK);
    }
}
