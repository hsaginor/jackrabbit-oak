package org.apache.jackrabbit.oak.plugins.value;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Random;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.value.BlobReadOnlyChannelWrapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobByteChannelTest {

    private static final String TEST_FILE_DIR = "BlobByteChannelTestFiles";
    private static final String TEST_ZIP_NAME = "testzip";
    private static final long TEST_FILE_SIZE = 1000000; 
    
    private static File testDir;
    private static File testFile;
    private static File zipDir;
    private static File testZipFile;
    private static SeekableByteChannel testChannel;
    
    @BeforeClass
    public static void setUpClass() throws IOException {
        testFile = geterateTestFile(TEST_FILE_DIR);
        testDir = testFile.getParentFile();
        
        Path path = Paths.get(testFile.getAbsolutePath());
        testChannel = FileChannel.open(path, StandardOpenOption.READ);
        
        createTestZipFile();
    }
    
    private static void createTestZipFile() throws IOException {
        zipDir = new File(testDir, TEST_ZIP_NAME);
        zipDir.mkdir();
        zipDir.deleteOnExit();
        ArrayList<File> filesToZip = new ArrayList<File>();
        for (int i = 0; i < 10; i++) {
            File file = geterateTestFile(zipDir.getAbsolutePath());
            file.deleteOnExit();
            filesToZip.add(file);
        }

        testZipFile = new File(testDir, TEST_ZIP_NAME + ".zip");
        testZipFile.deleteOnExit();
        ArchiveOutputStream o = null;
        try {
            o = new ZipArchiveOutputStream(testZipFile);
            for (File f : filesToZip) {
                ArchiveEntry entry = o.createArchiveEntry(f, f.getName());
                o.putArchiveEntry(entry);
                if (f.isFile()) {
                    InputStream i = Files.newInputStream(f.toPath());
                    IOUtils.copy(i, o);
                }
                o.closeArchiveEntry();
            }
        } finally {
            o.finish();
        }
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        testChannel.close();
        testZipFile.delete();
        zipDir.delete();
        testFile.delete();
        testDir.delete();
    }
    
    @Before
    public void setUpMethod() throws IOException {
        testChannel.position(0);
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
        SeekableByteChannel channel = null;
        
        try {
            channel = createTestChannel(testFile);
            assertFullRead(channel);   
        } finally {
            if(channel != null)
                channel.close();
        }
    }
    
    @Test
    public void testPositionChangeStreamChannel() throws IOException {
        SeekableByteChannel channel = null;
        
        try {
            channel = createTestChannel(testFile);
            testPositionChange(channel);
        } finally {
            if(channel != null)
                channel.close();
        }
    }
    
    @Test
    public void testPerformances() throws IOException {
        int numberOfReads = 100000;
        int bufferSize = 8000;
        
        SeekableByteChannel testChannel = null;
        
        try {
            testChannel = createTestChannel(testFile);
            System.out.println("Blob channel read " + numberOfReads + " times in " + readTimer(testChannel, numberOfReads, bufferSize) + "ms.");
        } finally {
            if(testChannel != null)
                testChannel.close();
        }
        
        long readFileTotalTime = System.currentTimeMillis();
        for(int i=0; i<numberOfReads; i++) {
            testChannel.position(0);
            readFully(testChannel, bufferSize);
        }
        readFileTotalTime = System.currentTimeMillis() - readFileTotalTime;
        System.out.println("File channel read " + numberOfReads + " times in " + readFileTotalTime + "ms.");
    }
    
    @Test
    public void testUnzip() throws IOException {

        SeekableByteChannel fileChannel = null;
        SeekableByteChannel channel = null;

        ZipFile zipFile1 = null;
        ZipFile zipFile2 = null;

        try {
            Path path = Paths.get(testZipFile.getAbsolutePath());
            fileChannel = FileChannel.open(path, StandardOpenOption.READ);
            long size1 = fileChannel.size();

            channel = createTestChannel(testZipFile);
            long size2 = channel.size();

            assertEquals(size2, size1);

            zipFile1 = new ZipFile(fileChannel);
            zipFile2 = new ZipFile(channel);
            assertZipEntriesEquals(zipFile1, zipFile2);

        } finally {
            if (zipFile1 != null)
                zipFile1.close();
            if (zipFile2 != null)
                zipFile2.close();
            if (fileChannel != null)
                fileChannel.close();
            if (channel != null)
                channel.close();
        }

    }
    
    private void assertZipEntriesEquals(ZipFile zipFile1, ZipFile zipFile2) throws IOException {
        Enumeration<ZipArchiveEntry> e1 = zipFile1.getEntries();
        Enumeration<ZipArchiveEntry> e2 = zipFile2.getEntries();
        while (e1.hasMoreElements() && e2.hasMoreElements()) {
            ZipArchiveEntry entry1 = e1.nextElement();
            ZipArchiveEntry entry2 = e2.nextElement();
            assertEquals(entry1, entry2);
            assertStreamsEqual(zipFile1.getInputStream(entry1), zipFile2.getInputStream(entry2));
        }

        assertFalse("ZipArchiveEntry enumerations do not have equal number of entires.",
                e1.hasMoreElements() && e2.hasMoreElements());
    }
    
    private void assertStreamsEqual(InputStream in1, InputStream in2) throws IOException {
        try {
            assertTrue("Streams do not have the same data", IOUtils.contentEquals(in1, in2));
        } finally {
            in1.close();
            in2.close();
        }
    }
    
    private long readTimer(SeekableByteChannel channel, int numberOfReads, int bufferSize) throws IOException {
        long start = System.currentTimeMillis();
        
        for(int i=0; i<numberOfReads; i++) {
            channel.position(0);
            readFully(channel, bufferSize);
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
            long bytesReadFromStream = testChannel.read(testBuff);
            assertEquals(bytesReadFromChannel, bytesReadFromStream);
            assertArrayEquals(blobBytes, testBytes);
            blobBuff.rewind();
            testBuff.rewind();
            totalBytesRead += bytesReadFromStream;
        }
    }
    
    private void assertRead(SeekableByteChannel testBlobChannel, ByteBuffer blobBuff, ByteBuffer testBuff, long newPosition) throws IOException {
        testChannel.position(newPosition).position();
        testBlobChannel.position(newPosition).position();
        
        int blobBytesRead = testBlobChannel.read(blobBuff);
        int testBytesRead = testChannel.read(testBuff);
        assertEquals(blobBytesRead, testBytesRead);
        byte blobBytes[] = blobBuff.array();
        byte testBytes[] = testBuff.array();
        assertArrayEquals(blobBytes, testBytes);
    }
    
    private long readFully(SeekableByteChannel channel, int bufferSize) throws IOException {
        long start = System.currentTimeMillis();
        
        long totalBytesRead = 0;
        long bytesToRead = testFile.length();
        ByteBuffer buff = ByteBuffer.allocate(bufferSize);
        
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
    
    private SeekableByteChannel createTestChannel(final File blobFile) {
        Blob blob = mockBlob(blobFile);
        return new BlobByteChannel(blob);
    }
    
    private Blob mockBlob(final File blobFile) {
        Blob blob = mock(Blob.class);
        when(blob.length()).thenReturn(blobFile.length());
        when(blob.getNewStream()).then(new Answer<InputStream>() {
            @Override
            public InputStream answer(InvocationOnMock invocation) throws Throwable {
                InputStream in = new FileInputStream(blobFile);
                return new BufferedInputStream(in);
            }
        });
        return blob;
    }
    
    private static File geterateTestFile(String parentDir) throws IOException {
        File dir = new File(parentDir);
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

}
