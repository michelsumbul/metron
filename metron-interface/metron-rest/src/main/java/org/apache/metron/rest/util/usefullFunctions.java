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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author msumbul
 */
public class usefullFunctions {

    public static List<String> getCommandList(String cmd) {
        List<String> lCmd = new ArrayList<>();
        String[] cmdSplit = cmd.split(" ");

        for (int i = 0; i < cmdSplit.length; i++) {
            lCmd.add(cmdSplit[i]);

        }
        return lCmd;
    }

    public static long getCurrentNanoTime() {
        return System.nanoTime();
    }

    public static List<Path> getAllFiles(File curDir) {
        System.out.println("We are in list files");
        List<Path> listFiles = new ArrayList<>();
        File[] filesList = curDir.listFiles();
        for (File f : filesList) {

            if (f.isFile()) {
                // System.out.println(f.getAbsolutePath());
                listFiles.add(new Path(f.getAbsolutePath()));
            }
        }
        return listFiles;
    }

    public static String getMRJobHistoryRest(String query) {

        String resp = "Error";
        try {
            HttpHost target = new HttpHost("172.26.215.103", 8088, "http");
            HttpClient httpClient = HttpClientBuilder.create().build();

            // specify the get request
            HttpGet getRequest = new HttpGet("");
            HttpResponse httpResponse = httpClient.execute(target, getRequest);
            HttpEntity entity = httpResponse.getEntity();
            if (entity != null) {
                resp = EntityUtils.toString(entity);
            }
        } catch (IOException ex) {
            Logger.getLogger(usefullFunctions.class.getName()).log(Level.SEVERE, null, ex);
        }
        return resp;
    }

    public static String getMRJobHistoryRestState(String jobId) {

        String state = getMRJobHistoryRest("/ws/v1/cluster/apps/" + jobId + "/state");
        state = state.split(":")[1];
        state = state.replaceAll("\"", "").replaceAll("}", "");
        return state;
    }
}
