package de.uni_marburg.pdd_metadata.io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import de.uni_marburg.pdd_metadata.duplicate_detection.Magpie;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Block;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.keys.KeyElementFactory;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

public class DataReader {
    public String filePath;
    public String goldFilePath;
    public boolean hasHeadline;
    public char attributeSeparator;
    public int numLines;
    public int maxAttributes;
    private int numAttributes;
    private Charset charset;
    private boolean enablePartitionCaching = false;
    private String tempPath;
    private DataWriter dataWriter = new DataWriter();

    public DataReader(String filePath, Configuration config) {
        this.filePath = filePath;
        this.goldFilePath = "./data/" + config.getGoldStandardFileName();
        this.hasHeadline = config.isHasHeadline();
        this.attributeSeparator = config.getAttributeSeparator();
        this.numLines = config.getNumLines();
        this.maxAttributes = config.getMaxAttributes();
        this.charset = config.getCharset();
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

        if (!blocksToLoad.isEmpty()) {
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

        }
        return blocks;
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

    public Set<Duplicate> readResultDuplicates(Set<String> sampleIds) {
        try (CSVReader reader = this.buildFileReader(this.goldFilePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            Set<Duplicate> resultDuplicates = new HashSet<>();

            String[] ids;
            while ((ids = reader.readNext()) != null) {
                if (!sampleIds.isEmpty()) {
                    if (sampleIds.contains(ids[0]) && sampleIds.contains(ids[1])) {
                        resultDuplicates.add(new Duplicate(ids[0], ids[1], 0));
                    }
                } else {
                    resultDuplicates.add(new Duplicate(ids[0], ids[1], 0));
                }
            }

            return resultDuplicates;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> readResultDuplicatesSamples() {
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline, this.charset)) {
            Set<String> sampleIds = new HashSet<>();

            String[] ids;
            while ((ids = reader.readNext()) != null) {
                sampleIds.add(ids[0]);
            }

            return sampleIds;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public Map<String, List<String>> getAllColumnValues() {
        Map<String, List<String>> columnData = new HashMap<>();

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, false, this.charset)) {
            List<String[]> rows = reader.readAll();

            if (rows.isEmpty()) return columnData;

            String[] headers = rows.get(0);

            for (String header : headers) {
                columnData.put(header.trim(), new ArrayList<>());
            }

            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);
                for (int j = 0; j < headers.length; j++) {
                    if (row.length > j) {
                        columnData.get(headers[j].trim()).add(row[j].trim());
                    }
                }
            }
        } catch (CsvException | IOException e) {
            throw new RuntimeException(e);
        }

        return columnData;
    }

    public int getNumRecords() {
        if (this.numLines == 0) {
            this.numLines = this.countLines(this.filePath, this.charset);
        }

        return this.hasHeadline ? this.numLines - 1 : this.numLines;
    }

    private int getNumAttributes() {
        if (this.numAttributes == 0) {
            this.numAttributes = this.countAttributes();
        }

        return Math.min(this.numAttributes, this.maxAttributes);
    }

    private String[] extractFields(String[] fields, int[] indices) {
        String[] extractedFields = new String[indices.length];

        for (int i = 0; i < indices.length; ++i) {
            extractedFields[i] = indices[i] < fields.length ? fields[indices[i]] : "";
        }

        return extractedFields;
    }

    private int countAttributes() {
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
        int length = Math.min(sortedLineIndices.length, 5);
        StringBuilder buffer = new StringBuilder("SNM_Partition");

        for (int i = 0; i < length; ++i) {
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

    private int countLines(String filePath, Charset charset) {
        try (var lines = Files.lines(Path.of(filePath), charset)) {
            return (int) lines.count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Record fitToMaxSize(Record record) {
        return record.size <= this.maxAttributes ? record : new Record(record.index, Arrays.copyOfRange(record.values, 0, this.maxAttributes));
    }
}
