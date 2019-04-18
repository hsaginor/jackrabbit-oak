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
package org.apache.jackrabbit.oak.api.blob;

import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

/**
 * SeekableByteChannelBinary Interface. In addition to the normal JCR {@link Binary}
 * functionality, implementations of this class provide access to the blob through 
 * NIO Channel.
 * 
 * None that calls to write operations of the provided Channel will have no effect 
 * on the stored binary. Applications must use JCR API to persist changes.
 */
public interface ChannelBlob {

    /**
     * Returns SeekableByteChannel object for accessing this blob.
     * 
     * @return SeekableByteChannel
     */
    SeekableByteChannel createChannel();
    
    /**
     * Returns FileChannel object for accessing this blob.
     * 
     * @return FileChannel
     */
    FileChannel createFileChannel();
    
}
