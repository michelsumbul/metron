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
package org.apache.metron.rest.controller;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.metron.rest.RestException;
import org.apache.metron.common.Constants;
import org.apache.metron.pcap.PcapHelper;
import org.apache.metron.rest.model.PcapRequest;
import org.apache.metron.rest.service.impl.PcapQueryServiceAsyncImpl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.apache.metron.rest.util.PcapsResponse;
import org.apache.metron.rest.util.pcapQueryThread;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

/**
 *
 * @author msumbul
 */
@RestController
@RequestMapping("/api/v1/pcap")
public class PcapQueryController {

    List<pcapQueryThread> lPcapQueryThread = new ArrayList<>();

    //Rest api to sumbit complete async pcap query that will not keep an open connection to the client
    @ApiOperation(value = "Submit a pcap job to found specific paquets")
    @ApiResponses({
        @ApiResponse(message = "Return the id of the query running on the backend", code = 200)
    })
    @RequestMapping(value = "/pcapqueryfilterasync/submit", method = RequestMethod.POST)
    public ResponseEntity<String> submitAsyncPcapQuery(@RequestBody PcapRequest pcapRequest
    ) throws RestException, IOException {
        pcapQueryThread t = new pcapQueryThread(pcapRequest);
        lPcapQueryThread.add(t);
        t.start();
        return new ResponseEntity<>(t.getIdQuery(), HttpStatus.CREATED);
    }

    //Get status of a pcap query
    @ApiOperation(value = "Get status of a pcap query")
    @ApiResponses({
        @ApiResponse(message = "Return the status of the query running on the backend", code = 200)
    })
    @RequestMapping(value = "/pcapqueryfilterasync/status", method = RequestMethod.POST)
    public ResponseEntity<String> getAsyncPcapQueryStatus(@RequestParam(value = "idQuery") String idQuery
    ) throws RestException, IOException {

        pcapQueryThread t = pcapQueryThread.findQueryInList(lPcapQueryThread, idQuery);
        if (t == null) {
            return new ResponseEntity<>("Not Found", HttpStatus.NOT_FOUND);
        }

        return new ResponseEntity<>(t.getStatus(), HttpStatus.OK);
    }

    @ApiOperation(value = "Get the result of a pcap query")
    @ApiResponses({
        @ApiResponse(message = "Return the result of the query running on the backend", code = 200)
    })
    @RequestMapping(value = "/pcapqueryfilterasync/result", method = RequestMethod.POST)
    public ResponseEntity<PcapsResponse> getAsyncPcapQueryResult(@RequestParam(value = "idQuery") String idQuery
    ) throws RestException, IOException {

        pcapQueryThread t = pcapQueryThread.findQueryInList(lPcapQueryThread, idQuery);

        if(t == null){
            return new ResponseEntity<>(new PcapsResponse(), HttpStatus.NOT_FOUND);
        }
        if(t.getPcapsReponse() == null || t.getPcapsReponse().getResponseSize() == 0){
            return new ResponseEntity<>(new PcapsResponse(), HttpStatus.NO_CONTENT);
        }
        return new ResponseEntity<>(t.getPcapsReponse(), HttpStatus.OK);
    }

    @ApiOperation(value = "Clear query")
    @ApiResponses({
        @ApiResponse(message = "Clear the result of the query on backendside", code = 200)
    })
    @RequestMapping(value = "/pcapqueryfilterasync/clear", method = RequestMethod.POST)
    public ResponseEntity<String> clearAsyncPcapQueryResult(@RequestParam(value = "idQuery") String idQuery
    ) throws RestException, IOException {

        pcapQueryThread t = pcapQueryThread.findQueryInList(lPcapQueryThread, idQuery);
        lPcapQueryThread.remove(t);
        t.setPcapsReponse(new PcapsResponse());

        return new ResponseEntity<>("Done", HttpStatus.OK);
    }

    @ApiOperation(value = "List queries")
    @ApiResponses({
        @ApiResponse(message = "list the queries in memory on the rest api", code = 200)
    })
    @RequestMapping(value = "/pcapqueryfilterasync/listquery", method = RequestMethod.POST)
    public ResponseEntity<List<String>> getListQueries() throws RestException, IOException {

        return new ResponseEntity<>(pcapQueryThread.getListQueries(lPcapQueryThread), HttpStatus.OK);
    }

    /*
    @RequestMapping(value = "/pcapqueryfilter", method = RequestMethod.GET)
    ResponseEntity<List<PcapQueryResult>> request(@RequestParam(value = "sourceIP", defaultValue = "") String sourceIP,
            @RequestParam(value = "destinationIP", defaultValue = "") String destinationIP,
            @RequestParam(value = "sourcePort", defaultValue = "0") int sourcePort,
            @RequestParam(value = "destinationPort", defaultValue = "0") int destinationPort,
            @RequestParam(value = "protocol", defaultValue = "0") int protocol,
            @RequestParam(value = "et", defaultValue = "0") long et,
            @RequestParam(value = "st", defaultValue = "0") long st,
            @RequestParam(value = "dateFormat", defaultValue = " yyyy/MM/dd hh:mm:ss") String dateFormat,
            @RequestParam(value = "hdfsPathPcap", defaultValue = "/apps/metron/pcap") Path hdfsPathPcap,
            @RequestParam(value = "packetFiler", defaultValue = "") String packetFilter,
            @RequestParam(value = "includeReverse", defaultValue = "false") Boolean includeReverse
    ) throws RestException {

        List<PcapQueryResult> pcapQueryResult = new PcapQueryServiceImpl().QueryFilterUtility(sourceIP, destinationIP, sourcePort, destinationPort, protocol, et, st, dateFormat, hdfsPathPcap, packetFilter, includeReverse);
        return new ResponseEntity<>(pcapQueryResult, HttpStatus.OK);
    }

    @RequestMapping(value = "/pcapqueryfiltersync", method = RequestMethod.GET)
    public ResponseEntity getPcapsByIdentifiers(
            @RequestParam(value = "query") String query,
            @RequestParam(value = "startTime", defaultValue = "-1") long startTime,
            @RequestParam(value = "endTime", defaultValue = "-1") long endTime,
            @RequestParam(value = "numReducers", defaultValue = "10") int numReducers) throws RestException, IOException {

        PcapQueryServiceAsyncImpl queryAsync = new PcapQueryServiceAsyncImpl();
       // return queryAsync.getPcapsByIdentifiers(query, startTime, endTime, numReducers);
       return null;
    }

     */
    /////////////////////////
    /////////////////////////
    /*
    @RequestMapping(value = "/pcapqueryfilterasync", method = RequestMethod.GET)
    public DeferredResult<ResponseEntity> getPcapsByIdentifiersDeferred(
            @RequestParam(value = "srcIp") String srcIp,
            @RequestParam(value = "dstIp") String dstIp,
            @RequestParam(value = "protocol") String protocol,
            @RequestParam(value = "srcPort") String srcPort,
            @RequestParam(value = "dstPort") String dstPort,
            @RequestParam(value = "startTime", defaultValue = "-1") long startTime,
            @RequestParam(value = "endTime", defaultValue = "-1") long endTime,
            @RequestParam(value = "numReducers", defaultValue = "10") int numReducers,
            @RequestParam(value = "includeReverseTraffic", defaultValue = "false") boolean includeReverseTraffic,
            @RequestParam(value = "packetFilter", defaultValue = "") String packetFilter
    ) throws RestException, IOException {

        DeferredResult<ResponseEntity> deferred = new DeferredResult<>();
        new Thread(() -> {
            try {
        
                Map<String, String> query = new HashMap<String, String>() {
                    {
                        if (srcIp != null) {
                            put(Constants.Fields.SRC_ADDR.getName(), srcIp);
                        }
                        if (dstIp != null) {
                            put(Constants.Fields.DST_ADDR.getName(), dstIp);
                        }
                        if (srcPort != null) {
                            put(Constants.Fields.SRC_PORT.getName(), srcPort);
                        }
                        if (dstPort != null) {
                            put(Constants.Fields.DST_PORT.getName(), dstPort);
                        }
                        if (protocol != null) {
                            put(Constants.Fields.PROTOCOL.getName(), protocol);
                        }
                        put(Constants.Fields.INCLUDES_REVERSE_TRAFFIC.getName(), "" + includeReverseTraffic);
                        if (!org.apache.commons.lang3.StringUtils.isEmpty(packetFilter)) {
                            put(PcapHelper.PacketFields.PACKET_FILTER.getName(), packetFilter);
                        }
                    }
                };
                PcapQueryServiceAsyncImpl queryAsync = new PcapQueryServiceAsyncImpl();
                deferred.setResult(queryAsync.getPcapsByIdentifiers(query, startTime, endTime, numReducers));
            } catch (IOException | RestException ex) {
                Logger.getLogger(PcapQueryController.class.getName()).log(Level.SEVERE, null, ex);
            }
        }).start();

        return deferred;
    }
     */
    @ApiOperation(value = "Submit a pcap job to found specific paquet")
    @ApiResponses({
        @ApiResponse(message = "Return pcap object containing packets", code = 200)
    })
    @RequestMapping(value = "/pcapqueryfilterasync", method = RequestMethod.POST)
    public DeferredResult<ResponseEntity> getPcapsByIdentifiersDeferred(
            @RequestBody PcapRequest pcapRequest
    ) throws RestException, IOException {

        DeferredResult<ResponseEntity> deferred = new DeferredResult<>();
        new Thread(() -> {
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
                deferred.setResult(queryAsync.getPcapsByIdentifiers(query, pcapRequest.getStartTime(), pcapRequest.getEndTime(), pcapRequest.getNumReducers()));
            } catch (IOException | RestException ex) {
                Logger.getLogger(PcapQueryController.class.getName()).log(Level.SEVERE, null, ex);
            }
        }).start();

        return deferred;
    }

    private static boolean isValidPort(String port) {
        if (port != null && !port.equals("")) {
            try {
                Integer.parseInt(port);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

}
