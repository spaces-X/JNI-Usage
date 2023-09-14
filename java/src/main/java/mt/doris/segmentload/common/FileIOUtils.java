package mt.doris.segmentload.common;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileIOUtils {
    public static void packFilesToTarGz(String inputDirPath, String outputFilePath) throws IOException {
        File inputDir = new File(inputDirPath);
        File outputFile = new File(outputFilePath);

        try (FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(bos);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {

            File[] files = inputDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    addFileToTarGz(file, taos);
                }
            }
        }
    }
    private static void addFileToTarGz(File file, TarArchiveOutputStream taos) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            TarArchiveEntry entry = new TarArchiveEntry(file, file.getName());
            taos.putArchiveEntry(entry);
            IOUtils.copy(fis, taos);
            taos.closeArchiveEntry();
        }
    }

}
