package dsp;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SyslogParser {

    public static void main(String[] args) {
        printKeyValueCount("166ae799-1bca-4f16-9d7c-3f883ca3d6d6");
    }

    public static void printKeyValueCount(String uuid) {
        List<String> fileNames = getCounters(uuid);
        fileNames.forEach((s) -> System.out.println(s));
    }

    public static List<String> getCounters(String uuid) {
        String[] strings = new String[]{"","steps","","syslog.gz"};
        String path = "logs/testlog"+uuid+".log";
        List<String> fileNames = S3Utils.getFileNames(Constants.BUCKET_NAME, path);
        List<String> collect = fileNames.stream().filter((s) -> s.matches(".*steps.*syslog\\.gz")).collect(Collectors.toList());
        return collect;
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
