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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.metron.common.Constants;
import org.apache.metron.pcap.PcapHelper;
import org.apache.metron.rest.RestException;
import org.apache.metron.rest.config.PcapConfig;
import org.apache.metron.rest.model.PcapRequest;
import org.apache.metron.rest.service.impl.PcapQueryServiceAsyncImpl;
import org.apache.metron.rest.util.PcapsResponse;

/**
 *
 * @author msumbul
 */
public class pcapQueryThread extends Thread {

    private String idQuery;
    private String status;
    private long submitTime;
    private long endTime;
    private PcapsResponse pcapsReponse;
    private PcapRequest pcapRequest;

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
        runQueryFromCliLinuxProcess();
        setStatus("Finished");
        setEndTime(getCurrentNanoTime());

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

    }

    private long getCurrentNanoTime() {
        return System.nanoTime();
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
}
