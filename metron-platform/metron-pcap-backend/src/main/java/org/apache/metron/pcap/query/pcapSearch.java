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
package org.apache.metron.pcap.query;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.metron.common.Constants;
import org.apache.metron.common.system.Clock;
import org.apache.metron.common.utils.timestamp.TimestampConverters;
import org.apache.metron.pcap.PcapHelper;
import org.apache.metron.pcap.filter.PcapFilterConfigurator;
import org.apache.metron.pcap.filter.fixed.FixedPcapFilter;
import org.apache.metron.pcap.mr.PcapJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author msumbul
 */
public class pcapSearch {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    PcapJob pcapJob = new PcapJob();
    public static final CliConfig.PrefixStrategy PREFIX_STRATEGY = clock -> {
        String timestamp = new Clock().currentTimeFormatted("yyyyMMddHHmm");
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return String.format("%s-%s", timestamp, uuid);
    };
    private final CliConfig.PrefixStrategy prefixStrategy = PREFIX_STRATEGY;
    public String[] args;

    public static void main(String[] args) {

        pcapSearch pS = new pcapSearch(args);
        pS.run();
    }

    public pcapSearch(String[] args) {
        this.args = args;
    }

    public void run() {
        System.out.print(fixedQuery(args));

        
    }

    private String fixedQuery(String[] args) {
        try {
            Configuration hadoopConf = new Configuration();
            FixedCliParser queryParser = new FixedCliParser(prefixStrategy);
            FixedCliConfig config = null;
            CliConfig commonConfig = null;
            try {
                config = queryParser.parse(args);
                commonConfig = config;
            } catch (ParseException | java.text.ParseException e) {
                System.err.println(e.getMessage());
                queryParser.printHelp();

            }
            if (config.showHelp()) {
                queryParser.printHelp();

            }

            Pair<Long, Long> time = timeAsNanosecondsSinceEpoch(config.getStartTime(), config.getEndTime());
            long startTime = time.getLeft();
            long endTime = time.getRight();

            String jobId;
            jobId = queryAsync(
                    new Path(config.getBasePath()),
                    new Path(config.getBaseOutputPath()),
                    startTime,
                    endTime,
                    config.getNumReducers(),
                    config.getFixedFields(),
                    hadoopConf,
                    FileSystem.get(hadoopConf),
                    new FixedPcapFilter.Configurator());
            return jobId;
        } catch (IOException | ClassNotFoundException | InterruptedException ex) {
            java.util.logging.Logger.getLogger(pcapSearch.class.getName()).log(Level.SEVERE, null, ex);

        }
        return "error";
    }

    public <T> String queryAsync(Path basePath,
            Path baseOutputPath,
            long beginNS,
            long endNS,
            int numReducers,
            T fields,
            Configuration conf,
            FileSystem fs,
            PcapFilterConfigurator<T> filterImpl
    ) throws IOException, ClassNotFoundException, InterruptedException {

        String fileName = Joiner.on("_").join(beginNS, endNS, filterImpl.queryToString(fields), UUID.randomUUID().toString());

        if (LOG.isDebugEnabled()) {
            DateFormat format = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.LONG,
                    SimpleDateFormat.LONG
            );
            String from = format.format(new Date(Long.divideUnsigned(beginNS, 1000000)));
            String to = format.format(new Date(Long.divideUnsigned(endNS, 1000000)));
            LOG.debug("Executing query {} on timerange from {} to {}", filterImpl.queryToString(fields), from, to);
        }

        Path outputPath = new Path(baseOutputPath, fileName);
        Job job = pcapJob.createJob(basePath,
                outputPath,
                beginNS,
                endNS,
                numReducers,
                fields,
                conf,
                fs,
                filterImpl
        );
        if (job == null) {
            LOG.info("No files to process with specified date range.");
            return null;
        }
        job.submit();
        JobStatus jobStatus = job.getStatus();

        return jobStatus.getJobID().toString();

    }

    
    
    private Pair<Long, Long> timeAsNanosecondsSinceEpoch(long start, long end) {
        long revisedStart = start;
        if (revisedStart < 0) {
            revisedStart = 0L;
        }
        long revisedEnd = end;
        if (revisedEnd < 0) {
            revisedEnd = System.currentTimeMillis();
        }
        //convert to nanoseconds since the epoch
        revisedStart = TimestampConverters.MILLISECONDS.toNanoseconds(revisedStart);
        revisedEnd = TimestampConverters.MILLISECONDS.toNanoseconds(revisedEnd);
        return Pair.of(revisedStart, revisedEnd);
    }

    //Test to be deleted
    private String test(String[] args) {

        if (args[0].equals("-h")) {
            System.out.println("The parameter order is the following: inputFolder outputFolder beginNS endNS numReducers SRC_ADDR DST_ADDR SRC_PORT DST_PORT PROTOCOL INCLUDES_REVERSE_TRAFFIC");
        }
        if (args.length >= 11) {
            Path inputFolder = new Path(args[0]);
            Path outputFolder = new Path(args[1]);
            long beginNS = Long.valueOf(args[2]);
            long endNS = Long.valueOf(args[3]);
            int numReducers = Integer.valueOf(args[4]);
            Map<String, String> query = new HashMap<String, String>() {
                {
                    put(Constants.Fields.SRC_ADDR.getName(), args[5]);
                    put(Constants.Fields.DST_ADDR.getName(), args[6]);
                    put(Constants.Fields.SRC_PORT.getName(), args[7]);
                    put(Constants.Fields.DST_PORT.getName(), args[8]);
                    put(Constants.Fields.PROTOCOL.getName(), args[9]);
                    put(Constants.Fields.INCLUDES_REVERSE_TRAFFIC.getName(), "" + args[10]);
                }
            };
            PcapConfig pcapConfig = new PcapConfig();
            pcapConfig.setPcapSourcePath(inputFolder.toString());
            pcapConfig.setPcapOutputPath(outputFolder.toString());

            try {
                String jobId;
                jobId = queryAsync(inputFolder,
                        outputFolder,
                        beginNS,
                        endNS,
                        numReducers,
                        query,
                        pcapConfig.getConfiguration(),
                        FileSystem.get(pcapConfig.getConfiguration()),
                        new FixedPcapFilter.Configurator());
                return jobId;
            } catch (IOException | ClassNotFoundException | InterruptedException ex) {
                java.util.logging.Logger.getLogger(pcapSearch.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        return "Wrong number of arguments!";
    }
}
