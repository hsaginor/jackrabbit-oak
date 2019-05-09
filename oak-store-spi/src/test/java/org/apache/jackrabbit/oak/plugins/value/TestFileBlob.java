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
package org.apache.jackrabbit.oak.plugins.value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.value.BlobFileChannel;
import org.apache.jackrabbit.oak.plugins.value.BlobStreamChannel;
import org.jetbrains.annotations.NotNull;

/**
 * A blob as a file in the file system.
 * Used for testing.
 */
public class TestFileBlob implements Blob {

    protected final String path;

    public TestFileBlob(String path) {
        this.path = path;
    }

    @Override
    public String getReference() {
        return path; // FIXME: should be a secure reference
    }

    @Override
    public String getContentIdentity() {
        return null;
    }

    @NotNull
    @Override
    public InputStream getNewStream() {
        
        byte[] buff = new byte[8000];
        int bytesRead = 0;
        
        
        FileInputStream fin = null;
        ByteArrayOutputStream bao = null;
        try {
            fin = new FileInputStream(getFile());
            bao = new ByteArrayOutputStream();
            
            while((bytesRead = fin.read(buff)) != -1) {
                bao.write(buff, 0, bytesRead);
            }

            byte[] data = bao.toByteArray();

            return new ByteArrayInputStream(data);
             
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(fin != null)
                try {
                    fin.close();
                    bao.close();
                } catch (IOException e) {
                }
        }
    }

    @Override
    public long length() {
        return getFile().length();
    }

    private File getFile() {
        return new File(path);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TestFileBlob) {
            TestFileBlob other = (TestFileBlob) obj;
            return this.path.equals(other.path);
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
    
    @Override
    public SeekableByteChannel createChannel() {
        return new BlobStreamChannel(this);
    }

    @Override
    public FileChannel createFileChannel() throws IOException {
        return new BlobFileChannel(this);
    }
}
