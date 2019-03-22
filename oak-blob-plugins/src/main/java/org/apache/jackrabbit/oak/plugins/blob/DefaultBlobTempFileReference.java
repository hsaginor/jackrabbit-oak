package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.api.blob.BlobTempFileReference;

public class DefaultBlobTempFileReference implements BlobTempFileReference {

    @Override
    public File getTempFile(String prefixHint, String suffixHint) throws IOException {
        return null;
    }

    @Override
    public void close() {

    }

}
