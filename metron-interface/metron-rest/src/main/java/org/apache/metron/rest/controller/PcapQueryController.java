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
import java.util.List;
import org.apache.metron.rest.RestException;
import org.apache.metron.rest.model.PcapRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.apache.metron.rest.util.PcapsResponse;
import org.apache.metron.rest.util.pcapQueryThread;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
        System.out.println("We are in submit rest api fonction");
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
        
        if(!t.getStatus().equals("Finished")){
            return new ResponseEntity<>(new PcapsResponse(), HttpStatus.PROCESSING);
        }
        if(t == null){
            return new ResponseEntity<>(new PcapsResponse(), HttpStatus.NOT_FOUND);
        }
        if(t.getPcapsReponse() == null || t.getPcapsReponse().getResponseSize() == 0){
            return new ResponseEntity<>(new PcapsResponse(), HttpStatus.NO_CONTENT);
        }
        return new ResponseEntity<>(t.getPcapsReponse(), HttpStatus.OK);
    }

        @ApiOperation(value = "Get the result of a pcap query in a Json format")
    @ApiResponses({
        @ApiResponse(message = "Return the result of the query running on the backend in a JSON format", code = 200)
    })
    @RequestMapping(value = "/pcapqueryfilterasync/resultJson", method = RequestMethod.POST)
    public ResponseEntity<String> getAsyncPcapQueryResultInJson(@RequestParam(value = "idQuery") String idQuery
    ) throws RestException, IOException {

        pcapQueryThread t = pcapQueryThread.findQueryInList(lPcapQueryThread, idQuery);
        
        if(!t.getStatus().equals("Finished")){
            return new ResponseEntity<>("", HttpStatus.PROCESSING);
        }
        if(t == null){
            return new ResponseEntity<>("", HttpStatus.NOT_FOUND);
        }
        
        
        if(t !=null & t.getStatus().equals("Finished") ){
            t.downloadResultLocally();  //for complete async
            return new ResponseEntity<>(t.pdmlToJson(), HttpStatus.OK);
        }
        
        return new ResponseEntity<>("Error in the result collection.", HttpStatus.BAD_REQUEST);
        
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
