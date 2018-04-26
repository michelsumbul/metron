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

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.metron.common.hadoop.SequenceFileIterable;
import org.apache.metron.rest.model.PcapRequest;

/**
 *
 * @author msumbul
 */
public interface PcapQueryService {

    
    int runQueryFromCliLinuxProcessAsync();

    SequenceFileIterable readResults(Path outputPath, Configuration config, FileSystem fs);

    void writeLocally(SequenceFileIterable results, int numRecordsPerFile, Path outputFolder);

    String getPcapsLinuxProcessAsync();

    int pcapToPDML(Path pcapFile);

    String pdmlToJson();

    int updateYarnJobStatusRest();

    void downloadResultLocally();

    void deleteLocalData(File f);

    void deleteHDFSResult();

}
