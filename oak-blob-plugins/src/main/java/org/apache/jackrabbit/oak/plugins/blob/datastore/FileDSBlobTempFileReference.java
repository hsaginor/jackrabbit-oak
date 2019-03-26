/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.blob.TempFileReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileDSBlobTempFileReference implements TempFileReference {

    private static final Logger log = LoggerFactory.getLogger(FileDSBlobTempFileReference.class);
    
    private FileDataStore store;
    private String blobId;
    private File tempFile;
    
    FileDSBlobTempFileReference(FileDataStore store, String blobId) {
        this.store = store;
        this.blobId = blobId;
    }
    
    @Override
    public File getTempFile(String prefixHint, String suffixHint) throws IOException {
        if(tempFile == null) {
            FileOutputStream out = null;
            String blobPath = getBlobPath();
            
            try {
                Path source = Paths.get(blobPath, new String[] {});
                Path target = Files.createTempFile(prefixHint, suffixHint);
                tempFile = target.toFile();
                tempFile.deleteOnExit();
                
                out = new FileOutputStream(tempFile);
                Files.copy(source, out);
            } finally {
                if(out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        log.warn("Unable to close file output stream after creating a temp file for blob {}: {}",
                                new Object[] {blobPath, e.getMessage()});
                    }
                }
            }
        }
        
        return tempFile;
    }

    @Override
    public void close() {
        if(tempFile.exists()) {
            tempFile.delete();
        }
    }
    
    private String getBlobPath() {
        StringBuilder path = new StringBuilder(store.getPath()).append(File.separator);
        path.append(blobId.substring(0, 2)).append(File.separator);
        path.append(blobId.substring(2, 4)).append(File.separator);
        path.append(blobId.substring(4, 6)).append(File.separator);
        path.append(blobId.substring(0, blobId.indexOf('#')));
        return path.toString();
    }

}
