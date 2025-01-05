package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.progressive_blocking.BlockResult;
import de.pdd_metadata.duplicate_detection.structures.Record;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

@Getter
public class Blocking {
    public HashMap<Integer, Block> blocks;
    private int maxBlockRange;
    private int numLoadableBlocks;
    private DataReader dataReader;
    private double threshold;
    private int blockSize;
    private Levenshtein levenshtein = new Levenshtein();
    private Set<Duplicate> duplicates = new HashSet<>();
    private int partitionSize;
    private Sorter sorter;
    private KeyElementFactory keyElementFactory;

    public Blocking(int maxBlockRange, DataReader dataReader, double threshold, int blockSize, int partitionSize, Sorter sorter, KeyElementFactory keyElementFactory) {
        this.maxBlockRange = maxBlockRange;
        this.blockSize = blockSize;
        this.partitionSize = partitionSize;
        this.numLoadableBlocks = (int) Math.ceil((double) this.partitionSize / (double) this.blockSize);
        this.dataReader = dataReader;
        this.threshold = threshold;
        this.sorter = sorter;
        this.keyElementFactory = keyElementFactory;
    }

    public Set<BlockResult> findDuplicatesIn(HashMap<Integer, Record> records, int[] order, int keyId, int blockOffset) {
        int numBlocks = (int) Math.ceil((double) order.length / (double) this.blockSize);
        HashMap<Integer, Block> blocks = new HashMap<>();

        for (int blockId = 0; blockId < numBlocks; ++blockId) {
            Block block = new Block();
            int startOrderIndex = blockId * this.blockSize;
            int endOrderIndex = Math.min(startOrderIndex + this.blockSize, order.length);

            for (int orderIndex = startOrderIndex; orderIndex < endOrderIndex; ++orderIndex) {
                int lineId = order[orderIndex];
                block.records.put(lineId, records.get(lineId));
            }

            blocks.put(blockId + blockOffset, block);
        }

        Set<BlockResult> blockResults = new HashSet<>();

        blocks.forEach((blockId, block) -> {
            int numDuplicates = this.compare(block);
            if (this.maxBlockRange > 0) {
                blockResults.add(new BlockResult(blockId, blockId, numDuplicates, keyId));
            }
        });

        return blockResults;
    }

    protected void findDuplicatesUsingMultipleKeysSequential() throws IOException {
        for (int keyAttributeNumber : this.levenshtein.getSimilarityAttributes()) {
            int[] keyAttributeNumbers = new int[]{keyAttributeNumber};
            int[] order = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, keyAttributeNumbers, this.dataReader, this.partitionSize, this);
            this.runStandard(order);
        }

        System.out.println("Number of Duplicates: " + this.duplicates.size());
    }

    protected void findDuplicatesUsingMultipleKeysConcurrently() throws IOException {
        int numRecords = this.dataReader.getNumRecords();
        int numAttributes = this.levenshtein.getSimilarityAttributes().length;
        int numBlocks = (int) Math.ceil((double) numRecords / (double) this.blockSize);
        PriorityQueue<BlockResult> blockResults = new PriorityQueue<>();
        int[][] orders = new int[numAttributes][];

        for (int i = 0; i < this.levenshtein.getSimilarityAttributes().length; i++) {
            // int[] keyAttributeNumbers = new int[]{i};
            //order[i] = this.sort(this.levenshtein.getSimilarityAttributes()[i])
            orders[i] = this.sorter.calculateOrderRandom(this.dataReader);
            blockResults.addAll(this.runBasicBlocking(this.blockSize, numBlocks, this.numLoadableBlocks, orders[i], i));
        }

        // blockResults.stream().map(x -> x.getFirstBlockId() + " " + x.getSecondBlockId()).forEach(System.out::println);

        /*
        for (int keyAttributeNumber : this.levenshtein.getSimilarityAttributes()) {
            System.out.println(keyAttributeNumber);
            // int[] keyAttributeNumbers = new int[]{keyAttributeNumber};
            orders[keyAttributeNumber] = this.calculateOrderRandom();
            blockResults.addAll(this.runBasicBlocking(this.blockSize, numBlocks, this.numLoadableBlocks, orders[keyAttributeNumber], keyAttributeNumber));
        }

         */

        HashSet<Triple<Integer, Integer, Integer>> completedBlockPairs = new HashSet<>();

        while (!blockResults.isEmpty()) {
            List<Triple<Integer, Integer, Integer>> mostPromisingBlockPairs = new ArrayList<>();
            Set<Pair<Integer, Integer>> blocksToLoad = new HashSet<>();

            while (blocksToLoad.size() <= this.numLoadableBlocks - 4 && !blockResults.isEmpty()) {
                BlockResult blockResult = blockResults.remove();

                Triple<Integer, Integer, Integer> leftPair = new ImmutableTriple<>(blockResult.getFirstBlockId() - 1, blockResult.getSecondBlockId(), blockResult.getKeyId());
                Triple<Integer, Integer, Integer> rightPair = new ImmutableTriple<>(blockResult.getFirstBlockId(), blockResult.getSecondBlockId() + 1, blockResult.getKeyId());

                /*
                System.out.println("!completedBlockPairs.contains(leftPair): " + !completedBlockPairs.contains(leftPair));
                System.out.println("leftPair.getLeft() >= 0: " + leftPair.getLeft());
                System.out.println("!completedBlockPairs.contains(rightPair): " + !completedBlockPairs.contains(rightPair));
                System.out.println("rightPair.getMiddle() < numBlocks: " + rightPair.getMiddle());
                */

                if (!completedBlockPairs.contains(leftPair) && leftPair.getLeft() >= 0) {
                    completedBlockPairs.add(leftPair);
                    mostPromisingBlockPairs.add(leftPair);
                    blocksToLoad.add(new ImmutablePair<>(leftPair.getLeft(), leftPair.getRight()));
                    blocksToLoad.add(new ImmutablePair<>(leftPair.getMiddle(), leftPair.getRight()));
                }

                if (!completedBlockPairs.contains(rightPair) && rightPair.getMiddle() < numBlocks) {
                    completedBlockPairs.add(rightPair);
                    mostPromisingBlockPairs.add(rightPair);
                    blocksToLoad.add(new ImmutablePair<>(rightPair.getLeft(), rightPair.getRight()));
                    blocksToLoad.add(new ImmutablePair<>(rightPair.getMiddle(), rightPair.getRight()));
                }
            }

            //TODO ändert das Logik?
            if (blocksToLoad.isEmpty()) {
                System.out.println("No blocks to load!");
                break;
            }

            HashMap<Integer, HashMap<Integer, Block>> blocksPerKey = this.dataReader.readBlocks(orders, blocksToLoad, this.blockSize);

            for (Triple<Integer, Integer, Integer> pair : mostPromisingBlockPairs) {
                Block leftBlock = blocksPerKey.get(pair.getRight()).get(pair.getLeft());
                Block rightBlock = blocksPerKey.get(pair.getRight()).get(pair.getMiddle());
                int numDuplicates = this.compare(leftBlock, rightBlock);

                if (this.maxBlockRange > pair.getMiddle() - pair.getLeft()) {
                    blockResults.add(new BlockResult(pair.getLeft(), pair.getMiddle(), numDuplicates, pair.getRight()));
                }
            }
        }

        /*
        this.resultCollector.log(".................................... done! (Time: " + System.currentTimeMillis() + ")");
        this.resultCollector.log("Number of Results: " + this.resultCollector.getDuplicates().size());
        this.resultCollector.collectEvent("End: Blocking - " + this.method.name(), 1);
         */

        // this.duplicates.stream().map(x ->  x.getRecordId1() + " " + x.getRecordId2()).forEach(System.out::println);

        System.out.println("Number of Duplicates: " + this.duplicates.size());
    }

    private void runStandard(int[] order) {
        int numBlocks = (int) Math.ceil((double) order.length / (double) this.blockSize);
        int numLoadableBlocks = (int) Math.ceil((double) this.partitionSize / (double) this.blockSize);
        var test = this.runBasicBlocking(this.blockSize, numBlocks, numLoadableBlocks, order, -1);
    }

    private Set<BlockResult> runBasicBlocking(int blockSize, int numBlocks, int numLoadableBlocks, int[] order, int keyId) {
        Set<BlockResult> blockResults = new HashSet<>();

        for (int startBlock = 0; startBlock < numBlocks; startBlock += numLoadableBlocks) {
            int endBlock = Math.min(startBlock + numLoadableBlocks, numBlocks);
            this.blocks = this.dataReader.readBlocks(order, startBlock, endBlock, blockSize);

            blocks.forEach((blockId, block) -> {
                int numDuplicates = this.compare(block);
                if (this.maxBlockRange > 0) {
                    blockResults.add(new BlockResult(blockId, blockId, numDuplicates, keyId));
                }
            });
        }

        return blockResults;
    }

    private int compare(Block block) {
        int numDuplicates = 0;
        List<Integer> recordIds = new ArrayList<>(block.records.keySet());

        for (int recordId1 = 0; recordId1 < recordIds.size(); ++recordId1) {
            for (int recordId2 = recordId1 + 1; recordId2 < recordIds.size(); ++recordId2) {
                Record record1 = block.records.get(recordIds.get(recordId1));
                Record record2 = block.records.get(recordIds.get(recordId2));

                double value = this.levenshtein.calculateSimilarityOf(record1.values, record2.values);

                boolean newDuplicate = this.duplicates.stream().noneMatch(x -> x.getRecordId1() == record1.id && x.getRecordId2() == record2.id
                        || x.getRecordId1() == record2.id && x.getRecordId2() == record1.id);

                if (value >= threshold && newDuplicate) {
                    if (record1.values[0].equals("107667") && record2.values[0].equals("101414")) {
                        System.out.println(value);
                    }

                    this.duplicates.add(new Duplicate(record1.id, record2.id, Integer.parseInt(record1.values[record1.values.length - 1]), Integer.parseInt(record2.values[record2.values.length - 1])));
                    // this.duplicates.add(new Duplicate(record1.id, record2.id));
                    ++numDuplicates;
                }
            }
        }

        return numDuplicates;
    }

    private int compare(Block leftBlock, Block rightBlock) {
        int numDuplicates = 0;
        List<Integer> leftRecordIds = new ArrayList<>(leftBlock.records.keySet());
        List<Integer> rightRecordIds = new ArrayList<>(rightBlock.records.keySet());

        for (Integer recordId : leftRecordIds) {
            for (Integer id : rightRecordIds) {
                Record record1 = leftBlock.records.get(recordId);
                Record record2 = rightBlock.records.get(id);

                double value = this.levenshtein.calculateSimilarityOf(record1.values, record2.values);

                boolean newDuplicate = this.duplicates.stream().noneMatch(x -> x.getRecordId1() == record1.id && x.getRecordId2() == record2.id
                        || x.getRecordId1() == record2.id && x.getRecordId2() == record1.id);

                if (value >= threshold && newDuplicate) {
                    this.duplicates.add(new Duplicate(record1.id, record2.id, Integer.parseInt(record1.values[0]), Integer.parseInt(record2.values[0])));
                    ++numDuplicates;
                }
            }
        }

        return numDuplicates;
    }

    /*
    protected int[] sort(int[] keyAttributeNumbers) throws IOException {
        return this.sorter.calculateOrder(this.keyElementFactory, keyAttributeNumbers, this.sorterMethod);
    }

     */
}
