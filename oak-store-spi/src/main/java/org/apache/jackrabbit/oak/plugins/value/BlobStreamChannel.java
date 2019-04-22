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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import org.apache.jackrabbit.oak.api.Blob;

public class BlobStreamChannel implements SeekableByteChannel {

    private boolean isOpen = true;
    private Blob blob;
    private long position = 0;
    private InputStream stream;
    private ReadableByteChannel channel;
    
    public BlobStreamChannel(Blob blob) {
        this.blob = blob;
        this.stream = new BufferedInputStream(blob.getNewStream());
        this.channel = Channels.newChannel(stream);
        this.stream.mark(0);
    }
    
    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        checkClosed();
        if(stream != null) {
            stream.close();
        }
        isOpen = false;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        checkClosed();
        int bytesRead = channel.read(dst);
        position += bytesRead;
        return bytesRead;
    }
    
    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        checkClosed();
        
        if(newPosition > position) {
            long bytesToSkip = newPosition - position;
            if (bytesToSkip != stream.skip(newPosition - position)) {
                throw new IOException("Can't skip to position " + newPosition);
            } 
        } else {
            stream.reset();
            stream.mark(0);
            stream.skip(newPosition);
        }
        
        position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        return blob.length();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    private void checkClosed() throws IOException {
        if(!isOpen()) {
            throw new IOException("This channel has been closed.");
        }
    }

}
