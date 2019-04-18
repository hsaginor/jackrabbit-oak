package org.apache.jackrabbit.oak.segment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import org.apache.jackrabbit.oak.api.Blob;

public class BlobStreamChannel implements SeekableByteChannel {

    private boolean isOpen = true;
    private Blob blob;
    private long position = 0;
    private InputStream stream;
    
    BlobStreamChannel(Blob blob) {
        this.blob = blob;
        this.stream = blob.getNewStream();
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
        int bytesRead = 0;
        
        if(dst.hasArray()) {
            bytesRead = stream.read(dst.array());
        } else {
            byte tmpBuff[] = new byte[dst.capacity()];
            bytesRead = stream.read(tmpBuff);
            dst.put(tmpBuff);
            tmpBuff = null;
        }
        
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
            if (newPosition != stream.skip(newPosition - position)) {
                throw new IOException("Can't skip to position " + newPosition);
            } 
        } else if(position > newPosition) {
            stream.reset();
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
