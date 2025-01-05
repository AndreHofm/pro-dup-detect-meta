package de.pdd_metadata.duplicate_detection.io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import de.pdd_metadata.duplicate_detection.Magpie;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Record;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class DataReader {
    public String filePath;
    public boolean hasHeadline;
    public char attributeSeparator;
    public int numLines;
    public int maxAttributes;
    private int numAttributes;

    public DataReader(String filePath, boolean hasHeadline, char attributeSeparator, int numLines, int maxAttributes) {
        this.filePath = filePath;
        this.hasHeadline = hasHeadline;
        this.attributeSeparator = attributeSeparator;
        this.numLines = numLines;
        this.maxAttributes = maxAttributes;
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

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline)) {
            int resultLineIndex = 0;
            for (int i = 0; i < this.getNumRecords(); ++i) {
                String[] line = reader.readNext();

                if (i == sortedLineIndices[resultLineIndex]) {
                    Record record = new Record(i ,line);
                    record = this.fitToMaxSize(record);
                    int blockId = linePositions.get(i) / blockSize;
                    blocks.get(blockId).records.put(i, record);
                    // blocks.get(blockId).blockId = blockId;
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

    /*
    public HashMap<Integer, Block> transformToNCVR() {
        Map<Integer, Block> blocking = new HashMap<>();
        for (String[] record : records) {
            Integer key = calculateBlockingKeyFor(record);

            if (blocking.get(key) == null) {
                blocking.put(new ArrayList<>());
            }
            blocking.get(key).add(record);
        }

        return blocking;
     }
     */

    public List<Record> readLines(Collection<Integer> lineIndices) throws IOException {
        List<Integer> sortedLineIndices = new ArrayList<>(lineIndices);
        Collections.sort(sortedLineIndices);
        List<Record> resultRecords = new ArrayList<>();

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline)) {
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

        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline)){
            for(int i = 0; i < this.getNumRecords(); ++i) {
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
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline)){
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

    public int getNumRecords() throws IOException {
        if (this.numLines == 0) {
            this.numLines = this.countLines(this.filePath);
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

        for(int i = 0; i < indices.length; ++i) {
            extractedFields[i] = indices[i] < fields.length ? fields[indices[i]] : "";
        }

        return extractedFields;
    }

    protected int countAttributes() {
        try (CSVReader reader = this.buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline)) {
            String[] line;
            if ((line = reader.readNext()) == null) {
                throw new IOException("Failed to count the number of attributes while reading data: File is empty!");
            }

            return line.length;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CSVReader buildFileReader(String filePath, char attributeSeparator, boolean hasHeadline) throws IOException {
        return new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new CSVParserBuilder().withSeparator(attributeSeparator).withEscapeChar((char) 0).build())
                .withSkipLines(hasHeadline ? 1 : 0)
                .build();
    }

    private int countLines(String filePath) throws IOException {
        try (var lines = Files.lines(Path.of(filePath))) {
            return (int) lines.count();
        }
    }

    private Record fitToMaxSize(Record record) {
        return record.size <= this.maxAttributes ? record : new Record(record.id, Arrays.copyOfRange(record.values, 0, this.maxAttributes));
    }
}
