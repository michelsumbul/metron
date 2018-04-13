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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.metron.common.Constants;
import org.apache.metron.common.hadoop.SequenceFileIterable;
import org.apache.metron.pcap.PcapHelper;
import org.apache.metron.rest.RestException;
import org.apache.metron.rest.config.PcapConfig;
import org.apache.metron.rest.model.PcapRequest;
import org.apache.metron.rest.service.impl.PcapQueryServiceAsyncImpl;
import org.apache.metron.rest.util.PcapsResponse;
//import org.codehaus.jackson.JsonNode;
//import org.codehaus.jackson.map.ObjectMapper;
import org.apache.metron.rest.util.usefullFunctions;
import static org.apache.metron.rest.util.usefullFunctions.getCommandList;
import static org.apache.metron.rest.util.usefullFunctions.getCurrentNanoTime;
import org.springframework.core.env.Environment;

/**
 *
 * @author msumbul
 */
public class pcapQueryThread extends Thread {

    /*
    *The idQuery is not hte same as the jobId because it was the case we will have to wait until yarn send us 
    *back the ID which might take several seconds and block the connection to the rest client
     */
    private String idQuery;
    private String jobId;
    private String status;
    private long submitTime;
    private long endTime;
    private PcapsResponse pcapsReponse;
    private PcapRequest pcapRequest;
    private String pdml;
    private Path localPcapFile;
    private Environment environment;
    PcapQueryServiceAsyncImpl pcapQueryAsync;

    public pcapQueryThread(PcapRequest pcapRequest) {
        System.out.println("we are in method crete thread");
        this.submitTime = getCurrentNanoTime();
        this.pcapRequest = pcapRequest;
        this.idQuery = "pcapQuery_" + String.valueOf(getSubmitTime());

        this.status = "Created";

    }

    public void run() {

        System.out.println("Pcap Thread is running");
        setStatus("Running");

        //  runJobWithPcapJob();
        //working external process sync
        // runQueryFromCliLinuxProcess();
        //uncomment the following when we will have implemented to get dynamically the address of hte yarn server
        runQueryFromCliLinuxProcessAsynnc();

        setStatus("Finished");
        setEndTime(usefullFunctions.getCurrentNanoTime());

    }

    private void runJobWithPcapJob() {
        try {
            Map<String, String> query = new HashMap<String, String>() {
                {
                    if (pcapRequest.getSrcIp() != null) {
                        put(Constants.Fields.SRC_ADDR.getName(), pcapRequest.getSrcIp());
                    }
                    if (pcapRequest.getDstIp() != null) {
                        put(Constants.Fields.DST_ADDR.getName(), pcapRequest.getDstIp());
                    }
                    if (pcapRequest.getSrcPort() != null) {
                        put(Constants.Fields.SRC_PORT.getName(), pcapRequest.getSrcPort());
                    }
                    if (pcapRequest.getDstPort() != null) {
                        put(Constants.Fields.DST_PORT.getName(), pcapRequest.getDstPort());
                    }
                    if (pcapRequest.getProtocol() != null) {
                        put(Constants.Fields.PROTOCOL.getName(), pcapRequest.getProtocol());
                    }
                    put(Constants.Fields.INCLUDES_REVERSE_TRAFFIC.getName(), "" + pcapRequest.isIncludeReverseTraffic());
                    if (!org.apache.commons.lang3.StringUtils.isEmpty(pcapRequest.getPacketFilter())) {
                        put(PcapHelper.PacketFields.PACKET_FILTER.getName(), pcapRequest.getPacketFilter());
                    }
                }
            };
            PcapQueryServiceAsyncImpl queryAsync = new PcapQueryServiceAsyncImpl();
            System.out.println("Debug: We are going to call queryAsync.getPcapsByIdentifiersAsync ");
            PcapConfig pcapConfig = new PcapConfig();
            pcapConfig.setPcapOutputPath("/tmp/" + this.getIdQuery());
            setPcapsReponse(queryAsync.getPcapsByIdentifiersAsync(query, pcapRequest.getStartTime(), pcapRequest.getEndTime(), pcapRequest.getNumReducers(), pcapConfig));
        } catch (IOException ex) {
            Logger.getLogger(pcapQueryThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (RestException ex) {
            Logger.getLogger(pcapQueryThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void runQueryFromCliLinuxProcess() {
        PcapQueryServiceAsyncImpl queryAsync = new PcapQueryServiceAsyncImpl();
        setPcapsReponse(queryAsync.getPcapsLinuxProcess(pcapRequest, idQuery));

        List<Path> lPath = usefullFunctions.getAllFiles(new File("/tmp/" + this.idQuery));
        if (!lPath.isEmpty() && lPath != null) {
            this.localPcapFile = lPath.get(0);
            System.out.println("the local pcap file: " + this.localPcapFile.toString());
            pcapToPDML(this.localPcapFile);
        } else {
            this.localPcapFile = null;
        }

    }

    private void runQueryFromCliLinuxProcessAsynnc() {
        pcapQueryAsync = new PcapQueryServiceAsyncImpl();
        this.setJobId(pcapQueryAsync.getPcapsLinuxProcessAsync(pcapRequest, idQuery));
    }

    
    
    public void downloadResultLocally() {  
         
        try {
            Configuration config = new Configuration();
          SequenceFileIterable seqFile = pcapQueryAsync.readResults(new Path("/tmp/"+this.idQuery), config,FileSystem.get(config));
          pcapQueryAsync.writeLocally(seqFile, 1,new Path( "/tmp/"+this.idQuery));
        } catch (IOException ex) {
            Logger.getLogger(pcapQueryThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        List<Path> lPath = usefullFunctions.getAllFiles(new File("/tmp/" + this.idQuery));
        if (!lPath.isEmpty() && lPath != null) {
            this.localPcapFile = lPath.get(0);
            System.out.println("the local pcap file: " + this.localPcapFile.toString());
            pcapToPDML(this.localPcapFile);
        } else {
            this.localPcapFile = null;
        }

    }

    public static pcapQueryThread findQueryInList(List<pcapQueryThread> lPcap, String idQuery) {

        for (pcapQueryThread t : lPcap) {
            if (t.getIdQuery().equals(idQuery)) {
                return t;
            }
        }
        return null;
    }

    public static List<String> getListQueries(List<pcapQueryThread> lPcap) {
        List<String> lQueries = new ArrayList<>();
        for (pcapQueryThread t : lPcap) {
            lQueries.add(t.getIdQuery());
        }
        return lQueries;
    }

    public static List<String> getListMRJobStatus() {

        return null;
    }

    public String getMRJobHistoryStateRest() {

        String state = usefullFunctions.getMRJobHistoryRestState("application_" + this.getJobId().replace("job_", ""));
        this.setStatus(state);
        return state;
    }

    private void pcapToPDML(Path pcapFile) {

        if (pcapFile != null) {
            try {
                String cmd;
                ProcessBuilder pb = new ProcessBuilder();
                Process process;
                //cmd = "tshark -r " + pcapFile.toString() + " -T pdml >" + this.idQuery + ".pdml";
                cmd = "/usr/sbin/tshark -r " + pcapFile.toString() + " -T pdml";
                System.out.println(cmd);
                List<String> lCmd = getCommandList(cmd);
                lCmd = getCommandList(cmd);
                pb.directory(new File("/tmp/" + idQuery));
                pb.command(lCmd);
                pb.redirectOutput(new File(pcapFile.getParent().toString() + "/" + this.idQuery + ".pdml"));
                pb.redirectError(new File(pcapFile.getParent().toString() + "/" + this.idQuery + ".error"));
                process = pb.start();
                process.waitFor();
                process.destroy();

            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(pcapQueryThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public String pdmlToJson() {

        try {
            File f = new File("/tmp/" + this.idQuery + "/" + idQuery + ".pdml");
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

    /**
     * @return the id
     */
    public String getIdQuery() {
        return idQuery;
    }

    /**
     * @param id the id to set
     */
    public void setId(String idQuery) {
        this.idQuery = idQuery;
    }

    /**
     * @return the status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * @return the pcapsReponse
     */
    public PcapsResponse getPcapsReponse() {
        return pcapsReponse;
    }

    /**
     * @param pcapsReponse the pcapsReponse to set
     */
    public void setPcapsReponse(PcapsResponse pcapsReponse) {
        this.pcapsReponse = pcapsReponse;
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
     * @return the pdml
     */
    public String getPdml() {
        return pdml;
    }

    /**
     * @param pdml the pdml to set
     */
    public void setPdml(String pdml) {
        this.pdml = pdml;
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
     * @return the jobId
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * @param jobId the jobId to set
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
}
