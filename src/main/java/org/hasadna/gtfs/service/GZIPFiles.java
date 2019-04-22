package org.hasadna.gtfs.service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class GZIPFiles {
    /**
     *
     * taken from: https://erikwramner.wordpress.com/2014/05/02/lazily-read-lines-from-gzip-file-with-java-8-streams/
     *
     * Get a lazily loaded stream of lines from a gzipped file, similar to
     * {@link Files#lines(java.nio.file.Path)}.
     *
     * @param path
     *          The path to the gzipped file.
     * @return stream with lines.
     */
    public static Stream<String> lines(Path path) {
        InputStream fileIs = null;
        BufferedInputStream bufferedIs = null;
        GZIPInputStream gzipIs = null;
        try {
            fileIs = Files.newInputStream(path);
            // Even though GZIPInputStream has a buffer it reads individual bytes
            // when processing the header, better add a buffer in-between
            bufferedIs = new BufferedInputStream(fileIs, 65535);
            gzipIs = new GZIPInputStream(bufferedIs);
        } catch (IOException e) {
            closeSafely(gzipIs);
            closeSafely(bufferedIs);
            closeSafely(fileIs);
            throw new UncheckedIOException(e);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipIs));
        return reader.lines().onClose(() -> closeSafely(reader));
    }

    private static void closeSafely(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
}
