package de.pdd_metadata.io;

import com.opencsv.CSVWriter;
import de.pdd_metadata.duplicate_detection.structures.Record;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class DataWriter {
    public static String CHARSET_NAME = "ISO-8859-1";

    public void writeTemp(String fileName, HashMap<Integer, Record> records) {
        CSVWriter bufferedWriter = null;

        try {
            bufferedWriter = buildFileWriter("temp" + File.separator + fileName, ';');

            for (Map.Entry<Integer, Record> entry : records.entrySet()) {
                bufferedWriter.writeNext(concat(String.valueOf(entry.getKey()), entry.getValue().values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.flush();
                    bufferedWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private static String[] concat(String first, String[] rest) {
        String[] result = new String[rest.length + 1];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }

    private static CSVWriter buildFileWriter(String filePath, char attributeSeparator) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }

        return new CSVWriter(new OutputStreamWriter(new FileOutputStream(file), Charset.forName(CHARSET_NAME)),
                attributeSeparator,
                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END);
    }
}
