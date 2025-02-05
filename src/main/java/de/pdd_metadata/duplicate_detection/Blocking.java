package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.progressive_blocking.BlockResult;
import de.pdd_metadata.duplicate_detection.structures.Record;

import java.io.IOException;
import java.util.*;

import de.pdd_metadata.similarity_measures.Levenshtein;
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

    protected void findDuplicatesUsingSingleKey() throws IOException {
        int[] order = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, new int[]{8, 2, 3}, this.dataReader, this.partitionSize, this, null);
        this.runPriority(order);
    }

    protected void findDuplicatesUsingMultipleKeysSequential() throws IOException {
        double startTime = System.currentTimeMillis();

        for (int keyAttributeNumber : this.levenshtein.getSimilarityAttributes()) {
            int[] keyAttributeNumbers = new int[]{keyAttributeNumber};
            int[] order = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, keyAttributeNumbers, this.dataReader, this.partitionSize, this, null);
            this.runPriority(order);
        }

        double endTime = System.currentTimeMillis();

        System.out.println((endTime - startTime) / 1000 + "s");

        System.out.println("Number of Duplicates: " + this.duplicates.size());
    }

    protected void findDuplicatesUsingMultipleKeysConcurrently() throws IOException {
        int numRecords = this.dataReader.getNumRecords();
        int numAttributes = this.levenshtein.getSimilarityAttributes().length;
        int numBlocks = (int) Math.ceil((double) numRecords / (double) this.blockSize);
        PriorityQueue<BlockResult> blockResults = new PriorityQueue<>();
        int[][] orders = new int[numAttributes][];


        for (int i = 0; i < this.levenshtein.getSimilarityAttributes().length; i++) {
            int[] keyAttributeNumbers = new int[]{i};
            orders[i] = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, keyAttributeNumbers, this.dataReader, this.partitionSize, this, null);
            blockResults.addAll(this.runBasicBlocking(this.blockSize, numBlocks, this.numLoadableBlocks, orders[i], i));
        }

        HashSet<Triple<Integer, Integer, Integer>> completedBlockPairs = new HashSet<>();

        while (!blockResults.isEmpty()) {
            List<Triple<Integer, Integer, Integer>> mostPromisingBlockPairs = new ArrayList<>();
            Set<Pair<Integer, Integer>> blocksToLoad = new HashSet<>();

            while (blocksToLoad.size() <= this.numLoadableBlocks - 4 && !blockResults.isEmpty()) {
                BlockResult blockResult = blockResults.remove();

                Triple<Integer, Integer, Integer> leftPair = new ImmutableTriple<>(blockResult.getFirstBlockId() - 1, blockResult.getSecondBlockId(), blockResult.getKeyId());
                Triple<Integer, Integer, Integer> rightPair = new ImmutableTriple<>(blockResult.getFirstBlockId(), blockResult.getSecondBlockId() + 1, blockResult.getKeyId());

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
    }

    private void runStandard(int[] order) {
        int numBlocks = (int) Math.ceil((double) order.length / (double) this.blockSize);
        int numLoadableBlocks = (int) Math.ceil((double) this.partitionSize / (double) this.blockSize);
        this.runBasicBlocking(this.blockSize, numBlocks, numLoadableBlocks, order, -1);
    }

    private void runPriority(int[] order) throws IOException {
        int numBlocks = (int) Math.ceil((double) order.length / (double) this.blockSize);
        PriorityQueue<BlockResult> blockResults = new PriorityQueue<>(this.runBasicBlocking(this.blockSize, numBlocks, this.numLoadableBlocks, order, -1));
        HashSet<Pair<Integer, Integer>> completedBlockPairs = new HashSet<>();

        while (!blockResults.isEmpty()) {
            List<Pair<Integer, Integer>> mostPromisingBlockPairs = new ArrayList<>();
            Set<Integer> blocksToLoad = new HashSet<>();

            while (blocksToLoad.size() <= this.numLoadableBlocks - 4 && !blockResults.isEmpty()) {
                BlockResult blockResult = blockResults.remove();
                Pair<Integer, Integer> leftPair = new ImmutablePair<>(blockResult.getFirstBlockId() - 1, blockResult.getSecondBlockId());
                Pair<Integer, Integer> rightPair = new ImmutablePair<>(blockResult.getFirstBlockId(), blockResult.getSecondBlockId() + 1);
                if (!completedBlockPairs.contains(leftPair) && leftPair.getLeft() >= 0) {
                    completedBlockPairs.add(leftPair);
                    mostPromisingBlockPairs.add(leftPair);
                    blocksToLoad.add(leftPair.getLeft());
                    blocksToLoad.add(leftPair.getRight());
                }

                if (!completedBlockPairs.contains(rightPair) && rightPair.getRight() < numBlocks) {
                    completedBlockPairs.add(rightPair);
                    mostPromisingBlockPairs.add(rightPair);
                    blocksToLoad.add(rightPair.getLeft());
                    blocksToLoad.add(rightPair.getRight());
                }
            }

            if (numBlocks > this.numLoadableBlocks) {
                this.blocks = null;
                this.blocks = this.dataReader.readBlocks(order, blocksToLoad, this.blockSize);
            }

            for (Pair<Integer, Integer> pair : mostPromisingBlockPairs) {
                Block leftBlock = this.blocks.get(pair.getLeft());
                Block rightBlock = this.blocks.get(pair.getRight());
                int numDuplicates = this.compare(leftBlock, rightBlock);
                if (this.maxBlockRange > pair.getRight() - pair.getLeft()) {
                    blockResults.add(new BlockResult(pair.getLeft(), pair.getRight(), numDuplicates, -1));
                }
            }
        }

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

                Duplicate duplicate = new Duplicate(record1.index, record2.index, record1.values[0], record2.values[0]);

                boolean newDuplicate = !this.duplicates.contains(duplicate);

                if (value >= threshold && newDuplicate) {
                    this.duplicates.add(duplicate);
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

                Duplicate duplicate = new Duplicate(record1.index, record2.index, record1.values[0], record2.values[0]);

                boolean newDuplicate = !this.duplicates.contains(duplicate);

                if (value >= threshold && newDuplicate) {
                    this.duplicates.add(duplicate);
                    ++numDuplicates;
                }
            }
        }

        return numDuplicates;
    }
}
