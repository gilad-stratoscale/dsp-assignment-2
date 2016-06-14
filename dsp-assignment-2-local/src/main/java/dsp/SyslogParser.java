package dsp;

import java.io.*;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class SyslogParser {

    public static void main(String[] args) {
        printKeyValueCount("166ae799-1bca-4f16-9d7c-3f883ca3d6d6");
    }

    public static void printKeyValueCount(String uuid) {
        List<String> fileNames = getSyslogFiles(uuid);

        File syslog=null;
        for (String file : fileNames) {
            try {
                syslog = S3Utils.downloadFile2(Constants.BUCKET_NAME, file);
            }
            catch (IOException e) {
                e.printStackTrace();
                continue;
            }
            String randomTempFileName = "tempfile"+ UUID.randomUUID();
            unGzip(randomTempFileName,syslog.getAbsolutePath());
            printFileData(randomTempFileName);
        }
    }

    private static void printFileData(String file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            String stage = null;
            while (true) {
                line = br.readLine();
                if (line==null) {
                    break;
                }
                if (line.matches("out\\d with recursive false")) {
                    Pattern p = Pattern.compile(".*?with recursive false.*");
                    Matcher m = p.matcher(line);
                    if (m.find()) {
                        stage = m.group(1);
                    }
                } else if (line.matches(".*Map output records.*")) {
                    String[] splits = line.split("=");
                    System.out.println("stage " + stage + " sent " + splits[1] + " pairs");
                }
            }
        } catch (IOException e) {
            System.out.println("failed to parse syslog: " + file);
            e.printStackTrace();
        }
    }

    public static List<String> getSyslogFiles(String uuid) {
        String[] strings = new String[]{"", "steps", "", "syslog.gz"};
        String path = "logs/testlog" + uuid + ".log";
        List<String> fileNames = S3Utils.getFileNames(Constants.BUCKET_NAME, path);
        List<String> result = fileNames.stream().filter((s) -> s.matches(".*steps.*syslog\\.gz")).collect(Collectors.toList());
        return result;
    }

    public static void unGzip(String outputFileName, String inputFileName){
        byte[] buffer = new byte[1024];
        try{
            GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(inputFileName));
            FileOutputStream out = new FileOutputStream(outputFileName);
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            gzis.close();
            out.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
/*
    private static List<String> search(String pwd, String[] filters, int index) {
        try {
            List<String> fileNames = S3Utils.getFileNames(Constants.BUCKET_NAME, pwd);
            List<String> result = new ArrayList<>();
            for (String filename : fileNames) {
                if (filters != null && index < filters.length && !filters[index].equals("") && !filename.equals(filters[index])) {
                    continue;
                }
                List<String> searchResults = search(filename, filters, index + 1);
                if (searchResults == null || searchResults.isEmpty()) {
                    result.add(filename);
                }
                else {
                    result.addAll(searchResults);
                }
            }
            return result;
        }
        catch(Exception e) {
            return null;
        }
    }*/


}