package org.apache.jackrabbit.oak.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;

public class BlobRangeResponseHandler {

    private Blob blob;
    private Range[] ranges;
    private int currentIndex = 0;
    private long contentLength = 0;
    private String contentType;
    private String boundary;
    
    BlobRangeResponseHandler(Blob blob, String rangeHeader, String contentType) {
        this.blob = blob;
        this.ranges = fromHeader(rangeHeader);
        this.contentType = contentType;
        this.boundary = genBoundary();
        for(Range range : ranges) {
            contentLength += range.length();
        }
    }
    
    private String genBoundary() {
        String chars = "0123456789abcdefghijklmnopqrstuvwxyzABCSDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuffer sb = new StringBuffer();
        for(int i=0; i<15; i++) {
            char nextChar = chars.charAt((int)(chars.length()*Math.random()));
            sb.append(nextChar);
        }
        return sb.toString();
    }
    
    int getCount() {
        return ranges.length;
    }
    
    void startResponse(HttpServletResponse response) {
        response.setStatus(206);
        if(ranges.length > 1) 
            response.setHeader("Content-Type", "multipart/byteranges; boundary="+boundary);
    }
    
    InputStream getStream() {
        return blob.getNewStream();
    }
    
    boolean hasNext() {
       return currentIndex < ranges.length;
    }
    
    void writeNextRange(HttpServletResponse response) throws IOException {
        if(!hasNext())
            return;
        
        long start = ranges[currentIndex].start;
        long end = ranges[currentIndex].end;
        long rangeLength = ranges[currentIndex].length();
        long pos = start;
        long totalBytesRead = 0;
        long bytesToRead = rangeLength;
        
        try {
            OutputStream out = response.getOutputStream();
            response.addHeader("Content-Range", "bytes " + start + "-"
                    + end + "/" + blob.length());
            if(this.contentType != null) 
                response.addHeader("Content-Type", this.contentType);
            
            int bufSize = 2028l <= rangeLength ? 2028 : (int) rangeLength;
            byte chunk[] = new byte[bufSize];
            
            if(ranges.length > 1) {
                if(start>0)
                    IOUtils.write("\n", out, "UTF-8");
                IOUtils.write("--"+this.boundary, out, "UTF-8");
                IOUtils.write("\n", out, "UTF-8");
                out.flush();
            }
            
            while (bytesToRead > 0) {
                if(bytesToRead < bufSize) {
                    chunk = new byte[(int)bytesToRead];
                }
                
                int bytesRead = read(chunk, pos);
                out.write(chunk);
                
                pos+=bytesRead;
                totalBytesRead += bytesRead;
                bytesToRead = rangeLength - totalBytesRead;
                if(bytesToRead < bufSize) {
                    chunk = new byte[(int)bytesToRead];
                }
            }
            
            out.flush();
            
        } finally {
            currentIndex++;
        }
    }
    
    private void printDebug(byte chunk[]) {
        StringBuffer sb = new StringBuffer();
        for(byte b : chunk)
            sb.append((char)b).append(' ');
        System.out.println(sb.toString());
    }
    
    void close(HttpServletResponse response) throws IOException {
        OutputStream out = response.getOutputStream();
        if(ranges.length > 1) {
            IOUtils.write("\n", out, "UTF-8");
            IOUtils.write("--"+this.boundary, out, "UTF-8");
            IOUtils.write("\n", out, "UTF-8");
        } 
        out.flush();
    }

    private int read(byte[] b, long position) throws IOException {
        InputStream stream = getStream();
        try {
            if (position != stream.skip(position)) {
                throw new IOException("Can't skip to position " + position);
            }
            return stream.read(b);
        } finally {
            stream.close();
        }
    }
    
    private Range[] fromHeader(String header) {
        String rangeValuesString = header.trim().substring("bytes=".length());
        String rangeValues[] = rangeValuesString.split(",");
        Range[] ranges = new Range[rangeValues.length];
        
        long fileLength = blob.length();
        
        for(int i=0; i<rangeValues.length; i++) {
            String rangeValue = rangeValues[i];
            long start, end;

            if (rangeValue.startsWith("-")) {
                end = fileLength - 1;
                start = fileLength - 1
                        - Long.parseLong(rangeValue.substring("-".length()).trim());
            } else {
                String[] range = rangeValue.split("-");
                start = Long.parseLong(range[0].trim());
                end = range.length > 1 ? Long.parseLong(range[1].trim())
                        : fileLength - 1;
            }
            if (end > fileLength - 1) {
                end = fileLength - 1;
            }
            ranges[i] = new Range(start, end);
        }
        
        return ranges;
    }
    
    private static class Range {
        long start = 0;
        long end = 0;
        long length = 0;
        
        Range(long start, long end) {
            this.start = start;
            this.end = end;
            this.length = end - start + 1;
        }
        
        long length() {
            return length;
        }
    }
}
