package de.pdd_metadata.duplicate_detection.io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import de.pdd_metadata.duplicate_detection.structures.Block;
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

        try (CSVReader reader = buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline)) {
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

    public int getNumRecords() throws IOException {
        if (this.numLines == 0) {
            this.numLines = countLines(this.filePath);
        }

        return this.hasHeadline ? this.numLines - 1 : this.numLines;
    }

    public List<Record> readLines(Collection<Integer> lineIndices) throws IOException {
        List<Integer> sortedLineIndices = new ArrayList<>(lineIndices);
        Collections.sort(sortedLineIndices);
        List<Record> resultRecords = new ArrayList<>();

        try (CSVReader reader = buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline)) {
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

    private static CSVReader buildFileReader(String filePath, char attributeSeparator, boolean hasHeadline) throws IOException {
        return new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new CSVParserBuilder().withSeparator(attributeSeparator).build())
                .withSkipLines(hasHeadline ? 1 : 0)
                .build();
    }

    private static int countLines(String filePath) throws IOException {
        try (var lines = Files.lines(Path.of(filePath))) {
            return (int) lines.count();
        }
    }

    private Record fitToMaxSize(Record record) {
        return record.size <= this.maxAttributes ? record : new Record(record.id, Arrays.copyOfRange(record.values, 0, this.maxAttributes));
    }
}
