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

import java.io.BufferedReader;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.metron.rest.MetronRestConstants;
import org.apache.metron.rest.model.PcapRequest;
import org.apache.metron.rest.model.PcapResponse;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.anyVararg;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.core.env.Environment;

/**
 *
 * @author msumbul
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(HttpHost.class)
@PowerMockIgnore("javax.net.ssl.*")
public class PcapQueryServiceImplTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private Environment environment;
    private PcapQueryServiceImpl pcapQueryService;
    private ProcessBuilder processBuilder;
    private Process process;
    private PcapRequest pcapRequest;

    private HttpGet getRequest;
    private HttpResponse httpResponse;
    private HttpEntity entity;
    private org.apache.http.client.HttpClient httpClient;
    private HttpHost target;
    private BufferedReader subProcessInputReader;

    private String json;

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        environment = mock(Environment.class);
        processBuilder = mock(ProcessBuilder.class);
        process = mock(Process.class);

        getRequest = mock(HttpGet.class);
        httpResponse = mock(HttpResponse.class);
        httpClient = mock(HttpClient.class);
        entity = mock(HttpEntity.class);
        target = mock(HttpHost.class);
        subProcessInputReader = mock(BufferedReader.class);

        pcapRequest = new PcapRequest();
        pcapRequest.setSrcIp("192.168.1.2");
        pcapRequest.setSrcPort("33029");
        pcapRequest.setDstIp("192.168.1.1");
        pcapRequest.setDstPort("22");
        pcapRequest.setStartTime(1l);
        pcapRequest.setEndTime(1524137395328l);
        pcapRequest.setProtocol("6");
        pcapRequest.setPacketFilter("false");

        pcapQueryService = new PcapQueryServiceImpl();
        pcapQueryService.setPcapRequest(pcapRequest);
        pcapQueryService.setOutPath("/tmp/1524137396666/");

    }

    @After
    public void tearDown() throws Exception {
    }

    /*
    @Test
    public void runQueryFromCliLinuxProcessAsyncTest() throws Exception {
        when(environment.getProperty(MetronRestConstants.METRON_PCAP_QUERY_SCRIPT_PATH_SPRING_PROPERTY)).thenReturn("/usr/hcp/current/metron/bin/pcap_query.sh");
        
        whenNew(ProcessBuilder.class).withParameterTypes(String[].class).withArguments(anyVararg()).thenReturn(processBuilder);
        when(processBuilder.start()).thenReturn(process);
        when(process.exitValue()).thenReturn(0);
        pcapQueryService.setSubmitTime(1524137396666l);
        //when(pcapQueryService.getSubmitTime()).thenReturn(1524137396666l);
        
        //Mockito.doReturn("job_1524137395328_1234").when(process.exitValue());
        when(subProcessInputReader.readLine()).thenReturn("job_1524137395328_1234");
       pcapQueryService.runQueryFromCliLinuxProcessAsync();
        PcapResponse pcapRep = pcapQueryService.getPcapReponse();
        assertEquals("application_1524137395328_1234", pcapRep.getIdQuery() );
        verifyNew(ProcessBuilder.class).withArguments("/usr/hcp/current/metron/bin/pcap_query.sh",
                "fixedAsync", " ", " --ip_src_addr", " 192.168.1.2", " --ip_src_port", " 33029", " --ip_dst_addr", " 192.168.1.1", " --ip_dst_port", " 22", " --protocol", " 6", " -et", " 1524137395328", " -st", " 1", " -bop", " /tmp/1524137396666");
        
    }
     
    @Test
    public void pcapToPDMLTest() throws Exception {
        when(environment.getProperty(MetronRestConstants.TSHARK_PATH_SPRING_PROPERTY)).thenReturn("/usr/bin/tshark");
        whenNew(ProcessBuilder.class).withParameterTypes(String[].class).withArguments(anyVararg()).thenReturn(processBuilder);
        when(processBuilder.start()).thenReturn(process);
        when(process.exitValue()).thenReturn(0);
        
        pcapQueryService.setOutPath("/tmp/1524137396666/");
        PcapResponse pcapRep = new PcapResponse();
        pcapRep.setIdQuery("application_1524137395328_1234");

     
        

        pcapQueryService.setPcapResponse(pcapRep);
        assertEquals(0, pcapQueryService.pcapToPDML(new Path("/tmp/1524137396666/file1.pcap")));
        
        verify(process).waitFor();
        verifyNew(ProcessBuilder.class).withArguments("/usr/bin/tshark", " -r", " /tmp/1524137396666/file1.pcap", " -T", " pdml");

    }
    */

 
    @Test
    public void updateYarnJobStatusRestTest() throws Exception{
        when(environment.getProperty(MetronRestConstants.YARN_RESSOURCE_MANAGER_URL_SPRING_PROPERTY)).thenReturn("node1");
        when(environment.getProperty(MetronRestConstants.YARN_RESSOURCE_MANAGER_PORT_SPRING_PROPERTY)).thenReturn("8088");

        //when(HttpClientBuilder.create().build()).thenReturn((CloseableHttpClient) httpClient);
       // httpClient = HttpClientBuilder.create().build();
        when(httpClient.execute(target, getRequest)).thenReturn(httpResponse);
        when(httpResponse.getEntity()).thenReturn(entity);
        when(EntityUtils.toString(entity)).thenReturn("{\"app\":{\"id\":\"application_1524137395328_1234\",\"user\":\"metron\",\"name\":\"metron-pcap-backend-0.4.1.1.4.1.0-18.jar\",\"queue\":\"default\",\"state\":\"FINISHED\",\"finalStatus\":\"SUCCEEDED\",\"progress\":100.0,\"trackingUI\":\"History\",\"trackingUrl\":\"http://ms-metron1.field.hortonworks.com:8088/proxy/application_1524137395328_1234/\",\"diagnostics\":\"\",\"clusterId\":1522924938685,\"applicationType\":\"MAPREDUCE\",\"applicationTags\":\"\",\"priority\":0,\"startedTime\":1523968238782,\"finishedTime\":1523968270194,\"elapsedTime\":31412,\"amContainerLogs\":\"http://ms-metron3.field.hortonworks.com:8042/node/containerlogs/container_e03_1522924938685_0068_01_000001/metron\",\"amHostHttpAddress\":\"ms-metron3.field.hortonworks.com:8042\",\"allocatedMB\":-1,\"allocatedVCores\":-1,\"runningContainers\":-1,\"memorySeconds\":201728,\"vcoreSeconds\":138,\"queueUsagePercentage\":0.0,\"clusterUsagePercentage\":0.0,\"preemptedResourceMB\":0,\"preemptedResourceVCores\":0,\"numNonAMContainerPreempted\":0,\"numAMContainerPreempted\":0,\"logAggregationStatus\":\"SUCCEEDED\",\"unmanagedApplication\":false,\"amNodeLabelExpression\":\"\"}}");
       
        pcapQueryService.updateYarnJobStatusRest();
        
        verify(pcapQueryService.getPcapReponse().getStatus()).equals("FINISHED");
        verify(pcapQueryService.getPcapReponse().getPercentage()).equals("100.0");
       
        
    }
     

}
