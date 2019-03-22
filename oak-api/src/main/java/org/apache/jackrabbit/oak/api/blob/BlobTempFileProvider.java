package org.apache.jackrabbit.oak.api.blob;

public interface BlobTempFileProvider {
    
    /**
     * Provides temporary file access to the binary trough BlobTempFileReference interface. 
     * This method may return null if implementation cannot provide temporary file access.
     * 
     * @return
     */
    BlobTempFileReference getTempFileReference(String blobId);
}
