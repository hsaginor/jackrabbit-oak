package org.apache.jackrabbit.oak.plugins.value;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlobChannelTest {

    private static final String TEST_FILE_DIR = "testfiles";
    private static final long TEST_FILE_SIZE = 10000; 
    
    private static File testDir;
    private static File testFile;
    private static FileChannel testFileChannel;
    private static SeekableByteChannel testByteChannel;
    
    @BeforeClass
    public static void setUpClass() throws IOException {
        testFile = geterateTestFile();
        testDir = testFile.getParentFile();
        // testBlob = new TestFileBlob(testFile.getAbsolutePath());
        
        Path path = Paths.get(testFile.getAbsolutePath());
        testFileChannel = FileChannel.open(path, StandardOpenOption.READ);
        testByteChannel = testFileChannel;
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        testFileChannel.close();
        testDir.delete();
    }
    
    @Before
    public void setUpMethod() throws IOException {
        testFileChannel.position(0);
    }
    
    @Test
    public void testReadStreamChannel() throws IOException {
        TestFileBlob testBlob = new TestFileBlob(testFile.getAbsolutePath());
        SeekableByteChannel channel = null;
        
        try {
            channel = testBlob.createChannel();
            assertFullRead(channel);   
        } finally {
            if(channel != null)
                channel.close();
        }
    }
    
    @Test
    public void testReadFileChannel() throws IOException {
        TestFileBlob testBlob = new TestFileBlob(testFile.getAbsolutePath());
        FileChannel channel = null;
        
        try {
            channel = testBlob.createFileChannel();
            assertFullRead(channel);   
        } finally {
            if(channel != null)
                channel.close();
        }
    }
    
    @Test
    public void testPositionChangeStreamChannel() throws IOException {
        TestFileBlob testBlob = new TestFileBlob(testFile.getAbsolutePath());
        SeekableByteChannel channel = null;
        
        try {
            channel = testBlob.createChannel();
            testPositionChange(testBlob.createChannel());
        } finally {
            if(channel != null)
                channel.close();
        }
    }
    
    @Test
    public void testPositionChangeFileChannel() throws IOException {
        TestFileBlob testBlob = new TestFileBlob(testFile.getAbsolutePath());
        FileChannel channel = null;
        
        try {
            channel = testBlob.createFileChannel();
            testPositionChange(channel);
        } finally {
            if(channel != null)
                channel.close();
        }
    }
    
    @Test
    public void testPerformances() throws IOException {
        int numberOfReads = 100000;
        TestFileBlob testBlob = new TestFileBlob(testFile.getAbsolutePath());
        
        SeekableByteChannel blobChannel = null;
        SeekableByteChannel blobWrapperChannel = null;
        FileChannel blobFileChannel = null;
        
        try {
            blobChannel = testBlob.createChannel();
            System.out.println("Blob channel read " + numberOfReads + " times in " + readTimer(blobChannel, numberOfReads) + "ms.");
        } finally {
            if(blobChannel != null)
                blobChannel.close();
        }
        
        try {
            blobWrapperChannel = new WrapperChannelBlob(testFile.getAbsolutePath()).createChannel();
            System.out.println("Blob wrapper channel read " + numberOfReads + " times in " + readTimer(blobWrapperChannel, numberOfReads) + "ms.");
        } finally {
            if(blobWrapperChannel != null) 
                blobWrapperChannel.close();
        }
        
        try {
            blobFileChannel = testBlob.createFileChannel();
            System.out.println("Blob FileChannel read " + numberOfReads + " times in " + readTimer(blobFileChannel, numberOfReads) + "ms.");
        } finally {
            if(blobFileChannel != null)
                blobFileChannel.close();
        }
        
        long readFileTotalTime = System.currentTimeMillis();
        for(int i=0; i<numberOfReads; i++) {
            testByteChannel.position(0);
            readFully(testByteChannel);
        }
        readFileTotalTime = System.currentTimeMillis() - readFileTotalTime;
        System.out.println("File channel read " + numberOfReads + " times in " + readFileTotalTime + "ms.");
    }
    
    private long readTimer(SeekableByteChannel channel, int numberOfReads) throws IOException {
        long start = System.currentTimeMillis();
        
        for(int i=0; i<numberOfReads; i++) {
            channel.position(0);
            readFully(channel);
        }
        
        return System.currentTimeMillis() - start;
    }
    
    public void testPositionChange(SeekableByteChannel testBlobChannel) throws IOException {
        ByteBuffer blobBuff = ByteBuffer.allocate(1000);
        ByteBuffer testBuff = ByteBuffer.allocate(1000);
        
        long testPositions[] = {testBlobChannel.size()-1, 0, 1101, 3001, 100, testBlobChannel.size()-1, testBlobChannel.size()+10, 0};
        for(long position : testPositions) {
            blobBuff.rewind();
            Arrays.fill(blobBuff.array(), (byte) 0);
            testBuff.rewind();
            Arrays.fill(testBuff.array(), (byte) 0);
            assertRead(testBlobChannel, blobBuff, testBuff, position);
        }
    }
    
    private void assertFullRead(SeekableByteChannel testBlobChannel) throws IOException {
        ByteBuffer blobBuff = ByteBuffer.allocate(100);
        ByteBuffer testBuff = ByteBuffer.allocate(100);
        byte blobBytes[] = blobBuff.array();
        byte testBytes[] = testBuff.array();
        
        long totalBytesRead = 0;
        long bytesToRead = testFile.length();
        
        while(totalBytesRead < bytesToRead) {
            long bytesReadFromChannel = testBlobChannel.read(blobBuff);
            long bytesReadFromStream = testByteChannel.read(testBuff);
            assertEquals(bytesReadFromChannel, bytesReadFromStream);
            assertArrayEquals(blobBytes, testBytes);
            blobBuff.rewind();
            testBuff.rewind();
            totalBytesRead += bytesReadFromStream;
        }
    }
    
    private void assertRead(SeekableByteChannel testBlobChannel, ByteBuffer blobBuff, ByteBuffer testBuff, long newPosition) throws IOException {
        long x = testByteChannel.position(newPosition).position();
        long y = testBlobChannel.position(newPosition).position();
        
        int blobBytesRead = testBlobChannel.read(blobBuff);
        int testBytesRead = testByteChannel.read(testBuff);
        assertEquals(blobBytesRead, testBytesRead);
        byte blobBytes[] = blobBuff.array();
        byte testBytes[] = testBuff.array();
        assertArrayEquals(blobBytes, testBytes);
    }
    
    private long readFully(SeekableByteChannel channel) throws IOException {
        long start = System.currentTimeMillis();
        
        long totalBytesRead = 0;
        long bytesToRead = testFile.length();
        ByteBuffer buff = ByteBuffer.allocate(1000);
        
        while(totalBytesRead < bytesToRead) {
            long bytesRead = channel.read(buff);
            // printArrays(buff.array());
            buff.rewind();
            totalBytesRead += bytesRead;
        }
        
        return System.currentTimeMillis() - start;
    }
    
    // method for debugging 
    private void printArrays(byte[]... arrays) {
        for(byte[] array : arrays) {
            System.out.print("[ ");
            for(byte b : array) {
                System.out.print(""+b+" ");
            }
            System.out.println("]");
        }
        System.out.println();
    }
    
    private static File geterateTestFile() throws IOException {
        File dir = new File(TEST_FILE_DIR);
        dir.mkdirs();
        dir.deleteOnExit();
        
        File file = new File(dir, "test"+System.currentTimeMillis()+".bin");
        file.deleteOnExit();
        FileOutputStream out = new FileOutputStream(file);
        Random rand = new Random();
        byte buff[] = new byte[1000];
        
        try {
            while(file.length() < TEST_FILE_SIZE) {
                rand.nextBytes(buff);
                out.write(buff);
            }
        } finally {
            out.close();
        }
        
        return file;
    }
    
    private class WrapperChannelBlob extends TestFileBlob {

        public WrapperChannelBlob(String path) {
            super(path);
        }
        
        @Override
        public SeekableByteChannel createChannel() {
            FileChannel channel;
            try {
                channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
                return new BlobReadOnlyChannelWrapper(channel);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
