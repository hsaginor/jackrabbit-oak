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
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlobChannelTest {

    private static final String TEST_FILE_DIR = "testfiles";
    private static final long TEST_FILE_SIZE = 10000; 
    
    private static File testDir;
    private static File testFile;
    private static TestFileBlob testBlob;
    private static FileChannel testFileChannel;
    private static SeekableByteChannel testByteChannel;
    
    @BeforeClass
    public static void setUpClass() throws IOException {
        testFile = geterateTestFile();
        testDir = testFile.getParentFile();
        testBlob = new TestFileBlob(testFile.getAbsolutePath());
        
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
        SeekableByteChannel testBlobChannel = testBlob.createChannel();
        
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
    
    @Test
    public void testPositionChangeStreamChannel() throws IOException {
        SeekableByteChannel testBlobChannel = testBlob.createChannel();
        long blobBytesRead;
        long testBytesRead;
        
        ByteBuffer blobBuff = ByteBuffer.allocate(1000);
        ByteBuffer testBuff = ByteBuffer.allocate(1000);
        byte blobBytes[] = blobBuff.array();
        byte testBytes[] = testBuff.array();
            
        blobBytesRead = testBlobChannel.read(blobBuff);
        testBytesRead = testByteChannel.read(testBuff);
        assertEquals(blobBytesRead, testBytesRead);
        assertArrayEquals(blobBytes, testBytes);
        
        long testPositions[] = {1101, 3001, 100};
        for(long position : testPositions) {
            blobBuff.rewind();
            testBuff.rewind();
            assertRead(testBlobChannel, blobBuff, testBuff, position);
        }
    }
    
    private void assertRead(SeekableByteChannel testBlobChannel, ByteBuffer blobBuff, ByteBuffer testBuff, long newPosition) throws IOException {
        testBlobChannel.position(newPosition);
        testByteChannel.position(newPosition);
        int blobBytesRead = testBlobChannel.read(blobBuff);
        int testBytesRead = testByteChannel.read(testBuff);
        assertEquals(blobBytesRead, testBytesRead);
        byte blobBytes[] = blobBuff.array();
        byte testBytes[] = testBuff.array();
        assertArrayEquals(blobBytes, testBytes);
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
                IOUtils.write(buff, out);
            }
        } finally {
            IOUtils.closeQuietly(out);
        }
        
        return file;
    }
}
