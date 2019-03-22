package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.blob.TempFileReference;

public class FileDSBlobTempFileReference implements TempFileReference {

    private FileDataStore store;
    private String blobId;
    private File tempFile;
    
    FileDSBlobTempFileReference(FileDataStore store, String blobId) {
        this.store = store;
        this.blobId = blobId;
    }
    
    @Override
    public File getTempFile(String prefixHint, String suffixHint) throws IOException {
        if(tempFile == null) {
            String blobPath = getBlobPath();
            Path source = Paths.get(blobPath, new String[] {});
            Path target = Files.createTempFile(prefixHint, suffixHint);
            target = Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            tempFile = target.toFile();
            tempFile.deleteOnExit();
        }
        return tempFile;
    }

    @Override
    public void close() {
        if(tempFile.exists()) {
            tempFile.delete();
        }
    }
    
    private String getBlobPath() {
        StringBuilder path = new StringBuilder(store.getPath()).append(File.separator);
        path.append(blobId.substring(0, 2)).append(File.separator);
        path.append(blobId.substring(2, 4)).append(File.separator);
        path.append(blobId.substring(4, 6)).append(File.separator);
        path.append(blobId.substring(0, blobId.indexOf('#')));
        return path.toString();
    }

}
