package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Block;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.BlockResult;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;

import java.io.IOException;
import java.util.*;

import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Getter
public class Blocking extends DuplicateDetector {
    public HashMap<Integer, Block> blocks;
    private final int maxBlockRange;
    private final int numLoadableBlocks;
    private final int blockSize;
    private final Logger log = LogManager.getLogger(Blocking.class);

    public Blocking(DataReader dataReader, ResultCollector resultCollector, Configuration config) {
        super(dataReader, resultCollector, config);
        this.maxBlockRange = config.getMaxBlockRange();
        this.blockSize = config.getBlockSize();
        this.numLoadableBlocks = (int) Math.ceil((double) this.partitionSize / (double) this.blockSize);
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

    public void findDuplicatesUsingMultipleKeysSequential() throws IOException {
        this.log.info("Starting Blocking...");
        double startTime = System.currentTimeMillis();

        for (int keyAttributeNumber : this.levenshtein.getSimilarityAttributes()) {
            int[] keyAttributeNumbers = new int[]{keyAttributeNumber};
            int[] order = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, keyAttributeNumbers, this.dataReader, this.partitionSize, this, null);
            this.runPriority(order);
        }

        double endTime = System.currentTimeMillis() - startTime;
        this.log.info("Ending Blocking - (Runtime: {}ms)", endTime);
    }

    public void findDuplicatesUsingMultipleKeysConcurrently() throws IOException {
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

                this.levenshtein.compare(record1, record2);
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

                this.levenshtein.compare(record1, record2);
            }
        }

        return numDuplicates;
    }
}
