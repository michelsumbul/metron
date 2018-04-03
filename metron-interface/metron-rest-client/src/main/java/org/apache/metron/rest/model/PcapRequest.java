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
package org.apache.metron.rest.model;

/**
 *
 * @author msumbul
 */
public class PcapRequest {

    private String srcIp;
    private String dstIp;
    private String protocol;
    private String srcPort;
    private String dstPort;
    private long startTime;
    private long endTime;
    private int numReducers;
    private boolean includeReverseTraffic;
    private String packetFilter;

    /**
     * @return the srcIp
     */
    public String getSrcIp() {
        return srcIp;
    }

    /**
     * @param srcIp the srcIp to set
     */
    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    /**
     * @return the dstIp
     */
    public String getDstIp() {
        return dstIp;
    }

    /**
     * @param dstIp the dstIp to set
     */
    public void setDstIp(String dstIp) {
        this.dstIp = dstIp;
    }

    /**
     * @return the protocol
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * @param protocol the protocol to set
     */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * @return the srcPort
     */
    public String getSrcPort() {
        return srcPort;
    }

    /**
     * @param srcPort the srcPort to set
     */
    public void setSrcPort(String srcPort) {
        this.srcPort = srcPort;
    }

    /**
     * @return the dstPort
     */
    public String getDstPort() {
        return dstPort;
    }

    /**
     * @param dstPort the dstPort to set
     */
    public void setDstPort(String dstPort) {
        this.dstPort = dstPort;
    }

    /**
     * @return the startTime
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @param startTime the startTime to set
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
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
     * @return the numReducers
     */
    public int getNumReducers() {
        return numReducers;
    }

    /**
     * @param numReducers the numReducers to set
     */
    public void setNumReducers(int numReducers) {
        this.numReducers = numReducers;
    }

    /**
     * @return the includeReverseTraffic
     */
    public boolean isIncludeReverseTraffic() {
        return includeReverseTraffic;
    }

    /**
     * @param includeReverseTraffic the includeReverseTraffic to set
     */
    public void setIncludeReverseTraffic(boolean includeReverseTraffic) {
        this.includeReverseTraffic = includeReverseTraffic;
    }

    /**
     * @return the packetFilter
     */
    public String getPacketFilter() {
        return packetFilter;
    }

    /**
     * @param packetFilter the packetFilter to set
     */
    public void setPacketFilter(String packetFilter) {
        this.packetFilter = packetFilter;
    }

    private boolean isValidPort(String port) {
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
