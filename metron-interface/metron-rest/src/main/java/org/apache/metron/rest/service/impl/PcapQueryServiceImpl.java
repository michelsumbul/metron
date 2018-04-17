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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.Iterables;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.metron.common.hadoop.SequenceFileIterable;
import org.apache.metron.rest.MetronRestConstants;
import org.apache.metron.rest.model.PcapRequest;
import org.apache.metron.rest.model.PcapResponse;
import org.springframework.stereotype.Service;
import org.apache.metron.rest.util.Pdml;
import org.apache.metron.rest.util.ResultsWriter;
import org.apache.metron.rest.util.pcapQueryThread;
import org.apache.metron.rest.util.usefullFunctions;
import static org.apache.metron.rest.util.usefullFunctions.getCommandList;
import static org.apache.metron.rest.util.usefullFunctions.getCurrentNanoTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

/**
 *
 * @author msumbul
 */
@Service
public class PcapQueryServiceImpl {

    private Configuration configuration;
    private String outPath;
    private long submitTime;
    private long endTime;
    private PcapResponse pcapReponse;
    private PcapRequest pcapRequest;
    private Path localPcapFile;

    
    private Environment environment;

    @Autowired
    public void setEnvironment(final Environment environment) {
        this.environment = environment;
    }

    public PcapQueryServiceImpl() {

    }

    public PcapQueryServiceImpl(PcapRequest pcapRequest) {
        this.submitTime = getCurrentNanoTime();
        this.pcapRequest = pcapRequest;
        this.pcapReponse = new PcapResponse();
        this.outPath = "/tmp/" + String.valueOf(getSubmitTime());
        this.pcapReponse.setStatus("Created");
        runQueryFromCliLinuxProcessAsync();
        updateYarnJobStatusRest();
        

    }

    public PcapQueryServiceImpl(Configuration configuration) {
        this.configuration = configuration;
    }

    private void runQueryFromCliLinuxProcessAsync() {

        this.getPcapReponse().setIdQuery(getPcapsLinuxProcessAsync().replace("job", "application"));
    }

    private String getPcapsLinuxProcessAsync() {
      
        //String pcapQueryScript = environment.getProperty(MetronRestConstants.METRON_PCAP_QUERY_SCRIPT_PATH_SPRING_PROPERTY);

        
        String cmdToExec = "/usr/hcp/current/metron/bin/pcap_query.sh fixedAsync ";
        //String cmdToExec = pcapQueryScript + " fixedAsync ";
        //Need to be changed by something dynamic depending on the environment
        
        if (!pcapRequest.getSrcIp().isEmpty()) {
            cmdToExec = cmdToExec + " --ip_src_addr " + pcapRequest.getSrcIp();
        }
        if (!pcapRequest.getSrcPort().isEmpty() && Long.valueOf(pcapRequest.getSrcPort()) > 0) {
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

        if (pcapRequest.getEndTime() > 0) {
            cmdToExec = cmdToExec + " -et " + pcapRequest.getEndTime();
        }

        cmdToExec = cmdToExec + " -st " + pcapRequest.getStartTime() + " -bop " + this.getOutPath();

        try {
            ProcessBuilder pb = new ProcessBuilder();

            Process process;
            List<String> lCmd = getCommandList("mkdir " + this.getOutPath());
            System.out.println(lCmd);
            pb.command(lCmd);
            process = pb.start();
            process.waitFor();
            pb.directory(new File(this.getOutPath()));

            lCmd = getCommandList(cmdToExec);
            System.out.println(lCmd);
            pb.command(lCmd);

            process = pb.start();
            BufferedReader subProcessInputReader
                    = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String jobId = null;
            
            while ((jobId = subProcessInputReader.readLine()) != null) {
                return jobId;
            }
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(PcapQueryServiceAsyncImpl.class.getName()).log(Level.SEVERE, null, ex);
        }

        return "Error in get the jobID of the mapreduce pcap search program.";
    }

    public void downloadResultLocally() {

        try {
            Configuration config = new Configuration();
            SequenceFileIterable seqFile = readResults(new Path(this.getOutPath()), config, FileSystem.get(config));
            writeLocally(seqFile, 100, new Path(this.getOutPath()));
        } catch (IOException ex) {
            Logger.getLogger(pcapQueryThread.class.getName()).log(Level.SEVERE, null, ex);
        }

        List<Path> lPath = usefullFunctions.getAllFiles(new File(this.outPath));
        if (!lPath.isEmpty() && lPath != null) {
            this.setLocalPcapFile(lPath.get(0));
            System.out.println("the local pcap file: " + this.getLocalPcapFile().toString());
            pcapToPDML(this.getLocalPcapFile());
            this.pcapReponse.setJson(pdmlToJson());
            deleteLocalData(new File(this.getLocalPcapFile().getParent().toString()));
        } else {
            this.setLocalPcapFile(null);
        }

    }
    
    private void deleteLocalData(File f){
        
        if(f.isFile()){
            f.delete();
        }
        else{
            try {
                FileUtils.deleteDirectory(f);
            } catch (IOException ex) {
                Logger.getLogger(PcapQueryServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void deleteHDFSResult(){
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(new Path(this.outPath), true);
        } catch (IOException ex) {
            Logger.getLogger(PcapQueryServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private SequenceFileIterable readResults(Path outputPath, Configuration config, FileSystem fs) throws IOException {

        List<Path> files = new ArrayList<>();
        //for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(outputPath, false); it.hasNext();) {
        for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(outputPath, true); it.hasNext();) {
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

    private void writeLocally(SequenceFileIterable results, int numRecordsPerFile, Path outputFolder) {
        ResultsWriter resultsWriter = new ResultsWriter();
        try {

            Iterable<List<byte[]>> partitions = Iterables.partition(results, numRecordsPerFile);
            int part = 1;
            if (partitions.iterator().hasNext()) {
                for (List<byte[]> data : partitions) {
                    String outFileName = outputFolder.toString() + "/" + String.format("pcap-data-%s+%04d.pcap", numRecordsPerFile, part++);
                    if (data.size() > 0) {
                        resultsWriter.write(data, outFileName);
                        
                    }
                }
            } else {
                System.out.println("No results returned.");
            }
        } catch (IOException e) {

            Logger.getLogger(PcapQueryServiceAsyncImpl.class.getName()).log(Level.SEVERE, null, e);

        }
    }

    private void pcapToPDML(Path pcapFile) {

        if (pcapFile != null) {
            try {
                String cmd;
                ProcessBuilder pb = new ProcessBuilder();
                Process process;
               
                //   String tsharkPath = environment.getProperty(MetronRestConstants.TSHARK_PATH_SPRING_PROPERTY);
                //   cmd = tsharkPath + " -r " + pcapFile.toString() + " -T pdml";
                cmd = "/usr/sbin/tshark -r " + pcapFile.toString() + " -T pdml";
                System.out.println(cmd);
                List<String> lCmd = getCommandList(cmd);
                lCmd = getCommandList(cmd);
                pb.directory(new File(this.getOutPath()));
                pb.command(lCmd);
                pb.redirectOutput(new File(pcapFile.getParent().toString() + "/" + this.getPcapReponse().getIdQuery() + ".pdml"));
                pb.redirectError(new File(pcapFile.getParent().toString() + "/" + this.getPcapReponse().getIdQuery() + ".error"));
                process = pb.start();
                process.waitFor();
                process.destroy();

            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(pcapQueryThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private String pdmlToJson() {

        try {
            File f = new File(this.getOutPath() + "/" + this.getPcapReponse().getIdQuery() + ".pdml");
            XmlMapper xmlMapper = new XmlMapper();

            Pdml node = xmlMapper.readValue(new FileInputStream(f), Pdml.class);
            ObjectMapper jsonMapper = new ObjectMapper();
            String json = jsonMapper.writeValueAsString(node);
            return json;

        } catch (IOException ex) {
            Logger.getLogger(pcapQueryThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "conversion error";
    }

    public  void updateYarnJobStatusRest() {
        
        try {
            HttpClient httpclient = HttpClientBuilder.create().build();
            //String ressourceManager = environment.getProperty(MetronRestConstants.YARN_RESSOURCE_MANAGER_URL_SPRING_PROPERTY);
            //int port = environment.getProperty(MetronRestConstants.YARN_RESSOURCE_MANAGER_PORT_SPRING_PROPERTY);
            HttpHost target = new HttpHost("172.26.215.103", 8088, "http");

            // specify the get request
            HttpGet getRequest = new HttpGet("/ws/v1/cluster/apps/" + this.pcapReponse.getIdQuery() + "/");
            HttpResponse httpResponse = httpclient.execute(target, getRequest);
            HttpEntity entity = httpResponse.getEntity();
            if (entity != null) {
                String json = EntityUtils.toString(entity);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode actualObj = mapper.readTree(json);
                Iterator<String> l =  actualObj.fieldNames();
                this.pcapReponse.setStatus(actualObj.get("app").get("state").toString().replaceAll("\"", ""));
                this.pcapReponse.setPercentage(actualObj.get("app").get("progress").toString().replaceAll("\"", ""));
               
            }
            EntityUtils.consume(entity);
                    

        } catch (IOException ex) {
            Logger.getLogger(PcapQueryServiceAsyncImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    /**
     * @return the configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * @param configuration the configuration to set
     */
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * @return the submitTime
     */
    public long getSubmitTime() {
        return submitTime;
    }

    /**
     * @param submitTime the submitTime to set
     */
    public void setSubmitTime(long submitTime) {
        this.submitTime = submitTime;
    }

    /**
     * @return the endTime
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * @param endTime the endTime to set
     */
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /**
     * @return the PcapResponse
     */
    public PcapResponse getPcapReponse() {
        return pcapReponse;
    }

    /**
     * @param pcapsReponse the pcapsReponse to set
     */
    public void setPcapResponse(PcapResponse pcapReponse) {
        this.pcapReponse = pcapReponse;
    }

    /**
     * @return the pcapRequest
     */
    public PcapRequest getPcapRequest() {
        return pcapRequest;
    }

    /**
     * @param pcapRequest the pcapRequest to set
     */
    public void setPcapRequest(PcapRequest pcapRequest) {
        this.pcapRequest = pcapRequest;
    }

    /**
     * @return the environment
     */
    public Environment getEnvironment() {
        return environment;
    }

    

    /**
     * @return the localPcapFile
     */
    public Path getLocalPcapFile() {
        return localPcapFile;
    }

    /**
     * @param localPcapFile the localPcapFile to set
     */
    public void setLocalPcapFile(Path localPcapFile) {
        this.localPcapFile = localPcapFile;
    }

    /**
     * @return the outPath
     */
    public String getOutPath() {
        return outPath;
    }

    /**
     * @param outPath the outPath to set
     */
    public void setOutPath(String outPath) {
        this.outPath = outPath;
    }

}
