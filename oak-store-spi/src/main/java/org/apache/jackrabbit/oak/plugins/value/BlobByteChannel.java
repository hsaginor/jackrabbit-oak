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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.IOUtils;

/**
 * 
 * SeekableByteChannel implementation for Blob access.
 *
 */
public class BlobByteChannel implements SeekableByteChannel {

    private static int MAX_READLIMIT = 1000000000;
    
    private Blob blob;
    private long position = 0;
    private InputStream stream;
    private ReadableByteChannel channel;
    
    public BlobByteChannel(Blob blob) {
        this.blob = blob;
        init();
    }
    
    private synchronized void init() {

        this.stream = getNewStream();

        int readlimit = MAX_READLIMIT;
        
        long size = blob.length();
        if (size < readlimit)
            readlimit = (int) size + 1;
        
        this.stream.mark(readlimit);

        ReadableByteChannel oldChannel = this.channel;
        this.channel = Channels.newChannel(stream);

        // This method might be called for larger assets in position(long newPosition) if
        // InputStream mark/reset position is invalidated.
        // So, we might need to close the old channel.
        if (oldChannel != null && oldChannel.isOpen()) {
            IOUtils.closeQuietly(oldChannel);
        }
    }
    
    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        if(channel != null && !channel.isOpen()) {
            channel.close();
        }
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
        if(newPosition < 0)
            throw new IllegalArgumentException("Cannot set position to " + newPosition);
        
        checkClosed();
        
        if(position == newPosition)
            return this;
        
        if(newPosition > position) {
            long bytesToSkip = newPosition - position;
            skip(bytesToSkip); 
        } else {
            // The reset can throw an IOException based on general contract of reset method of InputStream.
            // In this case we close current channel and create a new one to read from the blob.
            if(!tryReset()) {
                init();
            }
            
            if(newPosition > 0) 
                skip(newPosition);
        }
        
        position = newPosition;
        
        return this;
    }
    
    private InputStream getNewStream() {
        InputStream stream = blob.getNewStream();
        if(!stream.markSupported()) {
            stream = new BufferedInputStream(stream); 
        }
        return stream;
    }
    
    /**
     * This method attempts to compensate for the behavior of skip method in BufferedInputStream which states as follows. 
     * "The skip method may, for a variety of reasons, end up skipping over some smaller number of bytes, possibly 0 ..."
     * 
     * This is done by calling stream.skip multiple times until the specified number of bytes is skipped.
     * 
     * @param n
     * @return
     * @throws IOException 
     */
    private long skip(long n) throws IOException {
        
        long bytesToSkip = n;
        long totalBytesSkipped = 0;
       
        long skipped = stream.skip(bytesToSkip);
        totalBytesSkipped += skipped;
      
        while(bytesToSkip > skipped) {
            bytesToSkip = bytesToSkip-skipped;
            if(bytesToSkip > stream.available()) {
                break;
            }
            
            skipped = stream.skip(bytesToSkip);
            totalBytesSkipped += skipped;
        }
        
        return totalBytesSkipped;
    }
    
    private boolean tryReset() {
        try {
            stream.reset();
        } catch (IOException e) {
            return false;
        }
        
        return true;
    }

    @Override
    public long size() throws IOException {
        return blob.length();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    private void checkClosed() throws ClosedChannelException {
        if(!isOpen()) {
            throw new ClosedChannelException();
        }
    }

}
