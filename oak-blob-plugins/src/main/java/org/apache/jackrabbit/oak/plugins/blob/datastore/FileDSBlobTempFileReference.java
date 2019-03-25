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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.blob.TempFileReference;

public class FileDSBlobTempFileReference implements TempFileReference {

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
            String blobPath = getBlobPath();
            Path source = Paths.get(blobPath, new String[] {});
            Path target = Files.createTempFile(prefixHint, suffixHint);
            target = Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            tempFile = target.toFile();
            tempFile.deleteOnExit();
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
