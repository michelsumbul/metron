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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
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
import org.apache.metron.rest.model.PcapRequest;
import java.io.File;
import java.nio.file.FileSystems;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author msumbul We also reuse code from metron-api project
 */
public class PcapQueryServiceAsyncImpl {

    private static ThreadLocal<Configuration> CONFIGURATION = new ThreadLocal<Configuration>() {

        @Override
        protected Configuration initialValue() {
            Configuration config = new PcapConfig().getConfiguration();

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
            System.out.println("Query received: {}" + query);
            System.out.println("Configuration");
            System.out.println(CONFIGURATION.get());
            PcapConfig pcapConfig = new PcapConfig();
            System.out.println("Configuration mapreduce.framework.name " + pcapConfig.getConfiguration().get("mapreduce.framework.name"));
            results = getQueryUtil().query(new org.apache.hadoop.fs.Path(pcapConfig.getPcapSourcePath()),
                    new org.apache.hadoop.fs.Path(pcapConfig.getPcapOutputPath()),
                    startTime,
                    endTime,
                    numReducers,
                    query,
                    pcapConfig.getConfiguration(),
                    FileSystem.get(pcapConfig.getConfiguration()),
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

    public PcapsResponse getPcapsByIdentifiersAsync(Map<String, String> query, long startTime, long endTime, int numReducers, PcapConfig pcapConfig) throws IOException, RestException {
        PcapsResponse response = new PcapsResponse();
        SequenceFileIterable results = null;
        try {
            if (startTime < 0) {
                startTime = 0L;
            }
            if (endTime < 0) {
                endTime = System.currentTimeMillis();
            }

            //convert to nanoseconds since the epoch
            startTime = TimestampConverters.MILLISECONDS.toNanoseconds(startTime);
            endTime = TimestampConverters.MILLISECONDS.toNanoseconds(endTime);
            System.out.println("Query received: {}" + query);
            System.out.println("Configuration");
            System.out.println(CONFIGURATION.get());
            //   PcapConfig pcapConfig = new PcapConfig();

            System.out.println("Configuration mapreduce.framework.name " + pcapConfig.getConfiguration().get("mapreduce.framework.name"));
            System.out.println("Debug: We are going to call getQueryUtil().query in class pcapqueryserviceasyncimpl ");
            System.out.println("Debug: source path: " + pcapConfig.getPcapSourcePath());
            System.out.println("Debug: destination path: " + pcapConfig.getPcapOutputPath());
            PcapJob j = new PcapJob();
            /*  results = j.query(new org.apache.hadoop.fs.Path(pcapConfig.getPcapSourcePath()),
                     new org.apache.hadoop.fs.Path(pcapConfig.getPcapOutputPath()),
                     startTime,
                     endTime,
                     numReducers,
                     query,
                     pcapConfig.getConfiguration(),
                     FileSystem.get(pcapConfig.getConfiguration()),
                     new FixedPcapFilter.Configurator()
            );
             */
            System.out.println("Just before create job");
            Job jj = j.createJob(new org.apache.hadoop.fs.Path(pcapConfig.getPcapSourcePath()),
                    new org.apache.hadoop.fs.Path(pcapConfig.getPcapOutputPath()),
                    startTime,
                    endTime,
                    numReducers,
                    query,
                    pcapConfig.getConfiguration(),
                    FileSystem.get(pcapConfig.getConfiguration()),
                    new FixedPcapFilter.Configurator()
            );
            System.out.println("Just before submit");
            jj.submit();

            System.out.println("Just after submit");

            results = getQueryUtil().query(new org.apache.hadoop.fs.Path(pcapConfig.getPcapSourcePath()),
                    new org.apache.hadoop.fs.Path(pcapConfig.getPcapOutputPath()),
                    startTime,
                    endTime,
                    numReducers,
                    query,
                    pcapConfig.getConfiguration(),
                    FileSystem.get(pcapConfig.getConfiguration()),
                    new FixedPcapFilter.Configurator()
            );

            System.out.println("Debug: We finished the method getQueryUtil().query in class pcapqueryserviceasyncimpl ");
            System.out.println("Debug query function next line");
            response.setPcaps(results != null ? Lists.newArrayList(results) : null);
        } catch (Exception e) {
            System.err.println(e);
            throw new RestException(e);

        } finally {
            if (null != results) {
                results.cleanup();
            }
        }
        return response;
    }

    public PcapsResponse getPcapsLinuxProcess(PcapRequest pcapRequest, String idQuery) {
        SequenceFileIterable results = null;
        Runtime rt = Runtime.getRuntime();
        PcapsResponse response = new PcapsResponse();

        String cmdToExec = "/usr/hcp/current/metron/bin/pcap_query.sh fixed";

        if (!pcapRequest.getSrcIp().isEmpty()) {
            cmdToExec = cmdToExec + " --ip_src_addr " + pcapRequest.getSrcIp();
        }
        if (!pcapRequest.getSrcPort().isEmpty()) {
            cmdToExec = cmdToExec + " --ip_src_port " + pcapRequest.getSrcPort();
        }
        if (!pcapRequest.getDstIp().isEmpty()) {
            cmdToExec = cmdToExec + " --ip_dst_addr " + pcapRequest.getDstIp();
        }
        if (!pcapRequest.getDstPort().isEmpty() && Long.valueOf(pcapRequest.getDstPort()) > 0) {
            cmdToExec = cmdToExec + " --ip_dst_port " + pcapRequest.getDstPort();
        }

        if (!pcapRequest.getProtocol().isEmpty()) {
            cmdToExec = cmdToExec + " --protocol " + pcapRequest.getProtocol();
        }

        cmdToExec = cmdToExec + " -st " + pcapRequest.getStartTime() + " -bop /tmp/" + idQuery + " >>/tmp/log/pcapjob 2>&1";

        // rt.exec(cmdToExec);
        String workingDir = "/tmp/" + idQuery;
        try {
            ProcessBuilder pb = new ProcessBuilder();

            Process process;
            List<String> lCmd = getCommandList("mkdir " + workingDir);
            System.out.println(lCmd);
            pb.command(lCmd);
            process = pb.start();
            process.waitFor();
            pb.directory(new File(workingDir));

            lCmd = getCommandList(cmdToExec);
            System.out.println(lCmd);
            pb.command(lCmd);

            process = pb.start();
            process.waitFor();
            Configuration conf = new Configuration();
            results = readResults(new Path(workingDir), conf, FileSystem.get(conf));
        } catch (IOException ex) {
            Logger.getLogger(PcapQueryServiceAsyncImpl.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(PcapQueryServiceAsyncImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        response.setPcaps(results != null ? Lists.newArrayList(results) : null);

        return response;
    }

    private SequenceFileIterable readResults(String outputPath, Configuration config) throws IOException {
        List<Path> files = new ArrayList<>();
        files = getAllFiles(new File(outputPath));

        Collections.sort(files, (o1, o2) -> o1.getName().compareTo(o2.getName()));
        return new SequenceFileIterable(files, config);
    }

    private SequenceFileIterable readResults(Path outputPath, Configuration config, FileSystem fs) throws IOException {
        List<Path> files = new ArrayList<>();
        for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(outputPath, false); it.hasNext();) {
            Path p = it.next().getPath();
            if (p.getName().equals("_SUCCESS")) {
                fs.delete(p, false);
                continue;
            }
            files.add(p);
        }

        Collections.sort(files, (o1, o2) -> o1.getName().compareTo(o2.getName()));
        return new SequenceFileIterable(files, config);
    }

    private static List<Path> getAllFiles(File curDir) {
        System.out.println("We are in list files");
        List<Path> listFiles = new ArrayList<>();
        File[] filesList = curDir.listFiles();
        for (File f : filesList) {

            if (f.isFile()) {
                System.out.println(f.getAbsolutePath());
                listFiles.add(new Path(f.getAbsolutePath()));
            }
        }
        return listFiles;
    }

    private List<String> getCommandList(String cmd) {
        List<String> lCmd = new ArrayList<>();
        String[] cmdSplit = cmd.split(" ");

        for (int i = 0; i < cmdSplit.length; i++) {
            lCmd.add(cmdSplit[i]);

        }
        return lCmd;
    }
}
