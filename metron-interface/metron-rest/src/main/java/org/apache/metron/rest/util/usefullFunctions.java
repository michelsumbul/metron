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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;

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

}
