package de.pdd_metadata.duplicate_detection.io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Record;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class DataReader {
    public String filePath;
    public boolean hasHeadline;
    public char attributeSeparator;
    public int numLines;
    public int maxAttributes;

    public DataReader(String filePath, boolean hasHeadline,  char attributeSeparator, int numLines, int maxAttributes) {
        this.filePath = filePath;
        this.hasHeadline = hasHeadline;
        this.attributeSeparator = attributeSeparator;
        this.numLines = numLines;
        this.maxAttributes = maxAttributes;
    }

    public HashMap<Integer, Block> readBlocks(int[] order, int startBlock, int endBlock, int blockSize) {
        // assert order.length > 0;

        HashMap<Integer, Block> blocks = new HashMap<>();

        for (int blockId = startBlock; blockId < endBlock; ++blockId) {
            blocks.put(blockId, new Block(blockId, blockSize, new ArrayList<>(), new ArrayList<>(), new HashMap<>()));
        }

        if (startBlock >= endBlock) {
            return blocks;
        } else {
            int startIndex = startBlock * blockSize;
            int endIndex = Math.min(endBlock * blockSize, order.length);
            int[] sortedLineIndices = Arrays.copyOfRange(order, startIndex, endIndex);
            Arrays.sort(sortedLineIndices);
            HashMap<Integer, Integer> linePositions = new HashMap<>();

            System.out.println(Arrays.toString(sortedLineIndices));
            System.out.println(endIndex);

            for (int index = startIndex; index < endIndex; ++index) {
                linePositions.put(order[index], index);
            }


            HashMap<Integer, Block> var17;
            try {
                CSVReader reader = buildFileReader(this.filePath, this.attributeSeparator, this.hasHeadline);

                int resultLineIndex = 0;

                for (int i = 0; i < this.getNumRecords(); ++i) {
                    String[] line = reader.readNext();

                    Record record = new Record(line);

                    if (i == sortedLineIndices[resultLineIndex]) {
                        record = this.fitToMaxSize(record);

                        int blockId = linePositions.get(i) / blockSize;

                        blocks.get(blockId).records.put(i, record);

                        ++resultLineIndex;

                        if (resultLineIndex == sortedLineIndices.length) {
                            break;
                        }
                    }
                }

                var17 = blocks;

                reader.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return var17;
        }
    }

    public static CSVReader buildFileReader(String filePath, char attributeSeparator, boolean hasHeadline) throws IOException {
        return new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new CSVParserBuilder().withSeparator(attributeSeparator).build())
                .withSkipLines(hasHeadline ? 1 : 0)
                .build();
    }

    public int getNumRecords() throws IOException {
        if (this.numLines == 0) {
            this.numLines = countLines(this.filePath);
        }

        return this.hasHeadline ? this.numLines - 1 : this.numLines;
    }

    protected static int countLines(String filePath) throws IOException {
        try (var lines = Files.lines(Path.of(filePath))) {
            return (int) lines.count();
        }
    }

    protected Record fitToMaxSize(Record record) {
        return record.size <= this.maxAttributes ? record : new Record(Arrays.copyOfRange(record.values, 0, this.maxAttributes));
    }
}
