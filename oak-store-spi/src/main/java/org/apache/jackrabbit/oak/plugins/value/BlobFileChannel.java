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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.blob.TempFileReference;
import org.apache.jackrabbit.oak.api.blob.TempFileReferenceProvider;

/**
 * 
 * FileChannel that can be used to provide file access to blob. The implementation creates a temporary file and delegates all 
 * operations to it's FileChannel. No changes are persisted back to the repository. This must be done through the standard JCR API.
 * The temporary file is deleted when this FileChannel is closed.
 *
 */
public class BlobFileChannel extends FileChannel implements SeekableByteChannel {

    private FileChannel delegate;
    private File tempFile;
    private TempFileReference tmpFileRef;

    public BlobFileChannel(TempFileReferenceProvider fileProvider, String blobId) throws IOException {
        tmpFileRef = fileProvider.getTempFileReference(blobId);
        tempFile = tmpFileRef.getTempFile(null, null);
        Path path = Paths.get(tempFile.getAbsolutePath(), new String[] {});
        delegate = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }
    
    public BlobFileChannel(Blob blob) throws IOException {
        InputStream in = blob.getNewStream();
        Path target = Files.createTempFile("tmpOakBlob", null);
        Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        tempFile = target.toFile();
        tempFile.deleteOnExit();
        delegate = FileChannel.open(target, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }
    
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if(delegate != null) {
            return delegate.read(dst);
        }
        return 0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if(delegate != null) {
            return delegate.write(src);
        }
        return 0;
    }

    @Override
    public long position() throws IOException {
        if(delegate != null) {
            return delegate.position();
        }
        return 0;
    }

    @Override
    public long size() throws IOException {
        if(delegate != null) {
            return delegate.size();
        }
        return 0;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if(delegate != null) {
            return delegate.read(dsts, offset, length);
        }
        return 0;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if(delegate != null) {
            return delegate.write(srcs, offset, length);
        }
        return 0;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        if(delegate != null) {
            delegate.force(metaData);
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        if(delegate != null) {
            return delegate.transferTo(position, count, target);
        }
        return 0;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        if(delegate != null) {
            return delegate.transferFrom(src, position, count);
        }
        return 0;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        if(delegate != null) {
            return delegate.read(dst, position);
        }
        return 0;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if(delegate != null) {
            return delegate.write(src, position);
        }
        return 0;
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        if(delegate != null) {
            return delegate.map(mode, position, size);
        }
        return null;
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        if(delegate != null) {
            return delegate.lock(position, size, shared);
        }
        return null;
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        if(delegate != null) {
            return delegate.tryLock(position, size, shared);
        }
        return null;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if(tmpFileRef != null) {
            tmpFileRef.close();
            delegate.close();
        } else if(delegate != null) {
            delegate.close();
            tempFile.delete();
        }
        
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        if(delegate != null) {
            delegate.position(newPosition);
        }
        return null;
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        if(delegate != null) {
            delegate.truncate(size);
        }
        return null;
    }

}
