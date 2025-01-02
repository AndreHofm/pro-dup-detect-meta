package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.progressive_blocking.BlockResult;
import de.pdd_metadata.duplicate_detection.structures.Record;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class Blocking {
    public HashMap<Integer, Block> blocks;
    private int maxBlockRange;
    private int numLoadableBlocks;
    private DataReader dataReader;
    private double threshold;
    private int blockSize;
    private Levenshtein levenshtein = new Levenshtein();
    private Set<Pair<Record, Record>> duplicates = new HashSet<>();
    private int partitionSize;

    public Blocking(int maxBlockRange, DataReader dataReader, double threshold, int blockSize, int partitionSize) {
        this.maxBlockRange = maxBlockRange;
        this.blockSize = blockSize;
        this.partitionSize = partitionSize;
        this.numLoadableBlocks = (int) Math.ceil((double) this.partitionSize / (double) this.blockSize);
        this.dataReader = dataReader;
        this.threshold = threshold;
    }

    protected void findDuplicatesUsingMultipleKeysConcurrently() throws IOException {
        int numRecords = this.dataReader.getNumRecords();
        int numAttributes = this.levenshtein.getSimilarityAttributes().length;
        int numBlocks = (int) Math.ceil((double) numRecords / (double) this.blockSize);
        PriorityQueue<BlockResult> blockResults = new PriorityQueue<>();
        int[][] orders = new int[numAttributes][];

        for (int keyAttributeNumber : this.levenshtein.getSimilarityAttributes()) {
            // int[] keyAttributeNumbers = new int[]{keyAttributeNumber};
            orders[keyAttributeNumber] = this.calculateOrderRandom();
            blockResults.addAll(this.runBasicBlocking(this.blockSize, numBlocks, this.numLoadableBlocks, orders[keyAttributeNumber], keyAttributeNumber));
        }

        HashSet<Triple<Integer, Integer, Integer>> completedBlockPairs = new HashSet<>();

        while (!blockResults.isEmpty()) {
            List<Triple<Integer, Integer, Integer>> mostPromisingBlockPairs = new ArrayList<>();
            Set<Pair<Integer, Integer>> blocksToLoad = new HashSet<>();

            while (blocksToLoad.size() <= this.numLoadableBlocks - 4 && !blockResults.isEmpty()) {
                BlockResult blockResult = blockResults.remove();

                Triple<Integer, Integer, Integer> leftPair = new ImmutableTriple<>(blockResult.getFirstBlockId() - 1, blockResult.getSecondBlockId(), blockResult.getKeyId());
                Triple<Integer, Integer, Integer> rightPair = new ImmutableTriple<>(blockResult.getFirstBlockId(), blockResult.getSecondBlockId() + 1, blockResult.getKeyId());

                System.out.println(leftPair);
                System.out.println(rightPair);

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

        this.duplicates.stream().map(x -> Arrays.toString(x.getLeft().values) + " " + Arrays.toString(x.getRight().values)).forEach(System.out::println);

        System.out.println("Number of Duplicates: " + this.duplicates.size());
    }

    private Set<BlockResult> runBasicBlocking(int blockSize, int numBlocks, int numLoadableBlocks, int[] order, int keyId) throws IOException {
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

                double value = this.levenshtein.calculate(record1.values, record2.values);
                if (value >= threshold) {
                    this.duplicates.add(new ImmutablePair<>(record1, record2));
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

                double value = this.levenshtein.calculate(record1.values, record2.values);
                if (value >= threshold) {
                    this.duplicates.add(new ImmutablePair<>(record1, record2));
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

    private int[] calculateOrderRandom() throws IOException {
        int numRecords = this.dataReader.getNumRecords();
        List<Integer> order = new ArrayList<>(numRecords);

        for (int i = 0; i < numRecords; ++i) {
            order.add(i);
        }

        Collections.shuffle(order);
        int[] result = new int[numRecords];

        for (int i = 0; i < numRecords; ++i) {
            result[i] = order.get(i);
        }

        return result;
    }

}
