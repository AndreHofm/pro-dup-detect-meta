package de.pdd_metadata.io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvValidationException;
import de.pdd_metadata.similarity_measures.Levenshtein;
import de.pdd_metadata.duplicate_detection.Magpie;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Record;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

public class DataReader {
    public String filePath;
    public boolean hasHeadline;
    public char attributeSeparator;
    public int numLines;
    public int maxAttributes;
    private int numAttributes;
    private Charset charset;
    private boolean enablePartitionCaching = false;
    private String tempPath;
    private DataWriter dataWriter = new DataWriter();

    public DataReader(String filePath, boolean hasHeadline, char attributeSeparator, int numLines, int maxAttributes, Charset charset) {
        this.filePath = filePath;
        this.hasHeadline = hasHeadline;
        this.attributeSeparator = attributeSeparator;
        this.numLines = numLines;
        this.maxAttributes = maxAttributes;
        this.charset = charset;
    }

    public String[] getAttributeNames() {
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, false, this.charset)) {
            return reader.readNext();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<Integer, Record> readLines(int[] lineIndices) throws IOException {
        int[] sortedLineIndices = Arrays.copyOf(lineIndices, lineIndices.length);
        Arrays.sort(sortedLineIndices);
        String partitionKey = null;
        if (this.enablePartitionCaching) {
            partitionKey = this.getPartitionKeyFrom(sortedLineIndices);
            File cacheFile = new File(this.tempPath + partitionKey);
            if (cacheFile.exists()) {
                return this.readTemp(partitionKey, 0, lineIndices.length);
            }
        }

        HashMap<Integer, Record> records = new HashMap<>();
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            int resultLineIndex = 0;

            for (int i = 0; i < this.getNumRecords(); ++i) {
                String[] line = reader.readNext();
                if (i == sortedLineIndices[resultLineIndex]) {
                    Record record = new Record(i, line);
                    records.put(i, this.fitToMaxSize(record));
                    ++resultLineIndex;
                    if (resultLineIndex == sortedLineIndices.length) {
                        break;
                    }
                }
            }

            if (this.enablePartitionCaching) {
                this.dataWriter.writeTemp(partitionKey, records);
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

        return records;
    }

    public HashMap<Integer, Block> readBlocks(int[] order, int startBlock, int endBlock, int blockSize) {
        assert order.length > 0;

        System.out.println("Starting to read blocks...");
        if (startBlock >= endBlock) {
            return new HashMap<>();
        }

        HashMap<Integer, Block> blocks = new HashMap<>();
        for (int blockId = startBlock; blockId < endBlock; ++blockId) {
            blocks.put(blockId, new Block(blockId, blockSize, new HashMap<>()));
        }

        int startIndex = startBlock * blockSize;
        int endIndex = Math.min(endBlock * blockSize, order.length);

        int[] sortedLineIndices = Arrays.copyOfRange(order, startIndex, endIndex);
        Arrays.sort(sortedLineIndices);

        HashMap<Integer, Integer> linePositions = new HashMap<>();
        for (int index = startIndex; index < endIndex; ++index) {
            linePositions.put(order[index], index);
        }

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            int resultLineIndex = 0;
            for (int i = 0; i < this.getNumRecords(); ++i) {
                String[] line = reader.readNext();

                if (i == sortedLineIndices[resultLineIndex]) {
                    Record record = new Record(i, line);
                    record = this.fitToMaxSize(record);
                    int blockId = linePositions.get(i) / blockSize;
                    blocks.get(blockId).records.put(i, record);
                    ++resultLineIndex;

                    if (resultLineIndex == sortedLineIndices.length) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading blocks from file", e);
        }

        System.out.println("Finished reading blocks.");
        return blocks;
    }

    public HashMap<Integer, HashMap<Integer, Block>> readBlocks(int[][] orders, Set<Pair<Integer, Integer>> blocksToLoad, int blockSize) throws IOException {
        Set<Integer> lineIndices = new HashSet<>(orders[0].length);

        for (Pair<Integer, Integer> blockToLoad : blocksToLoad) {
            int keyId = blockToLoad.getRight();
            int startIndex = blockToLoad.getLeft() * blockSize;
            int endIndex = Math.min((blockToLoad.getLeft() + 1) * blockSize, orders[keyId].length);

            for (int index = startIndex; index < endIndex; ++index) {
                lineIndices.add(orders[keyId][index]);
            }
        }

        List<Record> records = this.readLines(lineIndices);
        HashMap<Integer, HashMap<Integer, Block>> blocksPerKey = new HashMap<>(orders.length);

        for (int keyId = 0; keyId < orders.length; ++keyId) {
            blocksPerKey.put(keyId, new HashMap<>());
        }

        for (Pair<Integer, Integer> blockToLoad : blocksToLoad) {
            int blockId = blockToLoad.getLeft();
            int keyId = blockToLoad.getRight();
            Block block = new Block();
            block.records = new HashMap<>();
            int blockStartIndex = blockId * blockSize;
            int blockEndIndex = Math.min((blockId + 1) * blockSize, orders[keyId].length);

            for (int blockIndex = blockStartIndex; blockIndex < blockEndIndex; ++blockIndex) {
                int lineId = orders[keyId][blockIndex];
                block.records.put(lineId, records.get(lineId));
            }

            blocksPerKey.get(keyId).put(blockId, block);
        }

        return blocksPerKey;
    }

    public HashMap<Integer, Block> readBlocks(int[] order, Set<Integer> blocksToLoad, int blockSize) throws IOException {
        assert order.length > 0;

        HashMap<Integer, Block> blocks = new HashMap<>();

        for (int blockId : blocksToLoad) {
            blocks.put(blockId, new Block());
        }

        if (blocksToLoad.isEmpty()) {
            return blocks;
        } else {
            List<Integer> sortedLineIndices = new ArrayList<>();
            HashMap<Integer, Integer> linePositions = new HashMap<>();

            for (int blockToLoad : blocksToLoad) {
                int startIndex = blockToLoad * blockSize;
                int endIndex = Math.min((blockToLoad + 1) * blockSize, order.length);

                for (int index = startIndex; index < endIndex; ++index) {
                    sortedLineIndices.add(order[index]);
                    linePositions.put(order[index], index);
                }
            }

            Collections.sort(sortedLineIndices);

            try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
                int resultLineIndex = 0;

                for (int i = 0; i < this.getNumRecords(); ++i) {
                    String[] line = reader.readNext();
                    if (i == sortedLineIndices.get(resultLineIndex)) {
                        Record record = new Record(i, line);
                        record = this.fitToMaxSize(record);
                        int blockId = linePositions.get(i) / blockSize;
                        blocks.get(blockId).records.put(i, record);
                        ++resultLineIndex;

                        if (resultLineIndex == sortedLineIndices.size()) {
                            break;
                        }
                    }
                }
            } catch (CsvValidationException e) {
                throw new RuntimeException(e);
            }

            return blocks;
        }
    }

    public List<Record> readLines(Collection<Integer> lineIndices) throws IOException {
        List<Integer> sortedLineIndices = new ArrayList<>(lineIndices);
        Collections.sort(sortedLineIndices);
        List<Record> resultRecords = new ArrayList<>();

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            int resultLineIndex = 0;

            for (int i = 0; i < this.getNumRecords(); ++i) {
                String[] line = reader.readNext();

                if (i == sortedLineIndices.get(resultLineIndex)) {
                    Record record = new Record(i, line);
                    resultRecords.add(i, this.fitToMaxSize(record));
                    ++resultLineIndex;
                    if (resultLineIndex == sortedLineIndices.size()) {
                        break;
                    }
                }
            }

        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

        return resultRecords;
    }

    public void readKeyAndLinesInto(Magpie magpie, KeyElementFactory keyElementFactory, int[] keyAttributeNumbers) throws IOException {
        assert keyAttributeNumbers.length > 0;

        assert keyAttributeNumbers.length <= this.getNumAttributes();

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            for (int i = 0; i < this.getNumRecords(); ++i) {
                Record record = new Record(reader.readNext());
                record = this.fitToMaxSize(record);
                String[] attributeValues = this.extractFields(record.values, keyAttributeNumbers);
                magpie.collectProgressive(keyElementFactory.create(i, attributeValues), record);
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Duplicate> readResultDuplicates() {
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            Set<Duplicate> resultDuplicates = new HashSet<>();

            String[] ids;
            while ((ids = reader.readNext()) != null) {
                resultDuplicates.add(new Duplicate(Integer.parseInt(ids[0]), Integer.parseInt(ids[1])));
            }

            return resultDuplicates;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Duplicate> simpleDuDe(Levenshtein levenshtein, float threshold) {
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            Set<Duplicate> resultDuplicates = new HashSet<>();
            List<Record> records = new ArrayList<>();

            String[] values;
            while ((values = reader.readNext()) != null) {
                records.add(new Record(Integer.parseInt(values[0]), values));
            }

            for (int i = 0; i < records.size(); i++) {
                for (int j = i; j < records.size(); j++) {
                    if (records.get(i).id != records.get(j).id) {
                        double sim = levenshtein.calculateSimilarityOf(records.get(i).values, records.get(j).values);
                        if (sim >= threshold) {
                            resultDuplicates.add(new Duplicate(records.get(i).id, records.get(j).id, Integer.parseInt(records.get(i).values[0]), Integer.parseInt(records.get(j).values[0])));
                        }
                    }
                }
            }

            return resultDuplicates;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<String, Block> readBlockForMulti(int order[]) {
        System.out.println("Starting to read blocks...");
        HashMap<String, Block> blocks = new HashMap<>();
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            Metaphone metaphone = new Metaphone();
            metaphone.setMaxCodeLen(4);

            for (int i = 0; i < this.getNumRecords(); ++i) {
                String[] line = reader.readNext();
                StringBuilder blockingKey = new StringBuilder();

                for (int key : order) {
                    String tempKey = metaphone.encode(line[key]);
                    String blockKey = tempKey.length() >= 2 ? tempKey.substring(0, 2) : tempKey;
                    blockingKey.append(blockKey);
                }

                String finalKey = blockingKey.toString();

                if (blocks.get(finalKey) == null) {
                    blocks.put(finalKey, new Block());
                }

                Record record = new Record(Integer.parseInt(line[0]), line);
                record = this.fitToMaxSize(record);
                if (blocks.get(finalKey).recordsA.size() <= blocks.get(finalKey).recordsB.size()) {
                    blocks.get(finalKey).recordsA.add(record);
                } else {
                    blocks.get(finalKey).recordsB.add(record);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading blocks from file", e);
        }

        System.out.println("Finished reading blocks.");
        return blocks;
    }


    public HashMap<String, Integer> countNullValues() {
        HashMap<String, Integer> nullCounts = new HashMap<>();


        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            String[] attributes = this.getAttributeNames();

            for (String attribute : attributes) {
                nullCounts.put(attribute.trim(), 0);
            }

            String[] values;
            while ((values = reader.readNext()) != null) {
                for (int i = 0; i < values.length && i < attributes.length; i++) {
                    if (values[i].trim().isEmpty()) {
                        nullCounts.put(attributes[i].trim(), nullCounts.get(attributes[i].trim()) + 1);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return nullCounts;
    }

    public Map<String, ClassName> detectColumnTypes() {
        Map<String, ClassName> columnTypes = new HashMap<>();

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            String[] firstRow = reader.readNext();
            String[] attributes = this.getAttributeNames();

            for (int i = 0; i < attributes.length; i++) {
                try {
                    Integer.parseInt(firstRow[i]);
                    columnTypes.put(attributes[i], ClassName.Integer);
                } catch (NumberFormatException e) {
                    try {
                        Double.parseDouble(firstRow[i]);
                        columnTypes.put(attributes[i], ClassName.Double);
                    } catch (NumberFormatException e2) {
                        columnTypes.put(attributes[i], ClassName.String);
                    }
                }
            }
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }


        return columnTypes;
    }


    public int getNumRecords() throws IOException {
        if (this.numLines == 0) {
            this.numLines = this.countLines(this.filePath, this.charset);
        }

        return this.hasHeadline ? this.numLines - 1 : this.numLines;
    }

    public int getNumAttributes() {
        if (this.numAttributes == 0) {
            this.numAttributes = this.countAttributes();
        }

        return Math.min(this.numAttributes, this.maxAttributes);
    }

    protected String[] extractFields(String[] fields, int[] indices) {
        String[] extractedFields = new String[indices.length];

        for (int i = 0; i < indices.length; ++i) {
            extractedFields[i] = indices[i] < fields.length ? fields[indices[i]] : "";
        }

        return extractedFields;
    }

    protected int countAttributes() {
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            String[] line;
            if ((line = reader.readNext()) == null) {
                throw new IOException("Failed to count the number of attributes while reading data: File is empty!");
            }

            return line.length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private HashMap<Integer, Record> readTemp(String fileName, int startIndex, int endIndex) {
        HashMap<Integer, Record> records = new HashMap<>();
        try (CSVReader reader = this.buildFileReader(this.tempPath + fileName, ';', false, this.charset)) {
            for (int i = 0; i < startIndex; ++i) {
                reader.readNext();
            }

            for (int i = 0; i < endIndex - startIndex; ++i) {
                String[] line = reader.readNext();
                records.put(Integer.parseInt((line[0])), new Record(Arrays.copyOfRange(line, 1, line.length)));
            }
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }

        return records;
    }

    private String getPartitionKeyFrom(int[] sortedLineIndices) {
        int lenght = Math.min(sortedLineIndices.length, 5);
        StringBuilder buffer = new StringBuilder("SNM_Partition");

        for (int i = 0; i < lenght; ++i) {
            buffer.append("-").append(sortedLineIndices[i]);
        }

        buffer.append("-(").append(sortedLineIndices.length).append(")");
        return buffer.toString();
    }

    private CSVReader buildFileReader(String filePath, char attributeSeparator, boolean hasHeadline, Charset charset) throws IOException {
        Path path = Paths.get(filePath);

        BufferedReader buffer = Files.newBufferedReader(path, charset);
        return new CSVReaderBuilder(buffer)
                .withCSVParser(new CSVParserBuilder()
                        .withSeparator(attributeSeparator)
                        .withQuoteChar('"')
                        .withEscapeChar('\\')
                        .withStrictQuotes(false)
                        .withIgnoreLeadingWhiteSpace(false)
                        .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_QUOTES)
                        .build())
                .withSkipLines(hasHeadline ? 1 : 0)
                .build();
    }

    private int countLines(String filePath, Charset charset) throws IOException {
        try (var lines = Files.lines(Path.of(filePath), charset)) {
            return (int) lines.count();
        }
    }

    private Record fitToMaxSize(Record record) {
        return record.size <= this.maxAttributes ? record : new Record(record.id, Arrays.copyOfRange(record.values, 0, this.maxAttributes));
    }

    private Class<?> detectType(String value) {
        if (value == null || value.isEmpty()) {
            return String.class;
        }

        try {
            return ClassUtils.getClass(value);
        } catch (ClassNotFoundException e) {
            return String.class;
        }
    }

    public IntArrayList readColumnAsList(int columnIndex) throws IOException {
        IntArrayList columnValues = new IntArrayList();

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // Überspringe Zeilen, die kürzer als der angeforderte Index sind
                if (columnIndex >= nextLine.length) {
                    continue;
                }

                String value = nextLine[columnIndex];
                if (value != null && !value.trim().isEmpty()) {
                    try {
                        // Konvertiere den Wert in einen Integer und füge ihn zur Liste hinzu
                        columnValues.add(Integer.parseInt(value.trim()));
                    } catch (NumberFormatException e) {
                        // Logge die Ausnahme, aber fahre mit der nächsten Zeile fort
                        System.err.println("Skipping non-integer value: " + value + " in column " + columnIndex);
                    }
                }
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

        return columnValues;
    }

    public enum ClassName {
        String,
        Integer,
        Double
    }
}
