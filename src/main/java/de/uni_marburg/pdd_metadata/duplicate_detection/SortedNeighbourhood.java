package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

@Getter
public class SortedNeighbourhood extends DuplicateDetector {
    private int windowSize;
    private int windowInterval;
    private ResultCollector resultCollector;
    private Logger log = LogManager.getLogger(SortedNeighbourhood.class);

    public SortedNeighbourhood(DataReader dataReader, ResultCollector resultCollector, Configuration config) {
        super(dataReader, resultCollector, config);
        this.windowSize = config.getWindowSize();
        this.windowInterval = config.getWindowInterval();
        this.resultCollector = resultCollector;
    }

    public int findDuplicatesIn(HashMap<Integer, Record> records, int[] order) {
        int numDuplicates = 0;

        for (int windowDistance = 1; windowDistance <= this.windowInterval; ++windowDistance) {

            for (int indexPivot = 0; indexPivot < records.size() - windowDistance; ++indexPivot) {
                int indexPartner = indexPivot + windowDistance;
                detectDuplicates(records, order, indexPivot, indexPartner);
            }
        }

        return numDuplicates;
    }

    public void findDuplicatesUsingSingleKey() throws IOException {
        this.log.info("Starting SNM...");
        var starTime = System.currentTimeMillis();

        int[] order = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, new int[]{8, 2, 3}, this.dataReader, this.partitionSize, null, this);
        this.runWindowLinearIncreasingWithLookahead(order);

        var endTime = System.currentTimeMillis() - starTime;
        this.log.info("Ending SNM - (Runtime: {}ms)", endTime);
    }

    public void findDuplicatesUsingMultipleKeysSequential() throws IOException {
        this.log.info("Starting SNM...");
        var starTime = System.currentTimeMillis();

        for (int keyAttributeNumber : this.levenshtein.getSimilarityAttributes()) {
            int[] keyAttributeNumbers = new int[]{keyAttributeNumber};
            int[] order = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, keyAttributeNumbers, this.dataReader, this.partitionSize, null, this);
            this.runWindowLinearIncreasingWithLookahead(order);
        }

        var endTime = System.currentTimeMillis() - starTime;
        this.log.info("Ending SNM - (Runtime: {}ms)", endTime);
    }

    public void findDuplicatesUsingMultipleKeysConcurrently() throws IOException {
        this.log.info("Starting SNM...");
        var starTime = System.currentTimeMillis();
        if (this.windowSize >= 2) {
            int numAttributes = this.levenshtein.getSimilarityAttributes().length;
            int[] keysLastResultCount = new int[numAttributes];
            int[] keysLastWindowSize = new int[numAttributes];
            int[][] orders = new int[numAttributes][];


            for (int i = 0; i < this.levenshtein.getSimilarityAttributes().length; i++) {
                int[] keyAttributeNumbers = new int[]{i};
                orders[i] = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, keyAttributeNumbers, this.dataReader, this.partitionSize, null, this);
                ;
                keysLastWindowSize[i] = 2;
                keysLastResultCount[i] = resultCollector.getDuplicates().size();
            }

            int bestNextKey;
            for (bestNextKey = 0; (bestNextKey = this.findBestNextKey(keysLastResultCount, keysLastWindowSize)) >= 0; keysLastResultCount[bestNextKey] = this.runSingleWindowDistance(orders[bestNextKey], keysLastWindowSize[bestNextKey] - 1)) {
                int var10002 = keysLastWindowSize[bestNextKey]++;
            }
        }

        var endTime = System.currentTimeMillis() - starTime;
        this.log.info("Ending SNM - (Runtime: {}ms)", endTime);
    }

    private void detectDuplicates(HashMap<Integer, Record> records, int[] order, int indexPivot, int indexPartner) {
        Record record1 = records.get(order[indexPivot]);
        Record record2 = records.get(order[indexPartner]);

        this.levenshtein.compare(record1, record2);
    }

    private void runWindowLinearIncreasing(int[] order) throws IOException {
        int localWindowInterval = this.partitionSize - this.windowSize + 1 <= order.length ? this.windowInterval : this.windowSize;

        for (int currentWindowStart = 1; currentWindowStart < this.windowSize; currentWindowStart += localWindowInterval) {
            int currentWindowEnd = Math.min(currentWindowStart + localWindowInterval, this.windowSize);
            if (currentWindowStart > this.windowInterval || localWindowInterval != this.windowInterval) {
                for (int partitionStartIndex = 0; partitionStartIndex < order.length; partitionStartIndex += this.partitionSize - this.windowSize + 1) {
                    int partitionEndIndex = Math.min(partitionStartIndex + this.partitionSize, order.length);
                    int[] partitionIndices = Arrays.copyOfRange(order, partitionStartIndex, partitionEndIndex);
                    HashMap<Integer, Record> records = this.dataReader.readLines(partitionIndices);
                    int lastPivotElement = partitionStartIndex + this.partitionSize - this.windowSize + 1 < order.length ? partitionIndices.length - this.windowSize + 1 : partitionIndices.length;

                    for (int windowDistance = currentWindowStart; windowDistance < currentWindowEnd; ++windowDistance) {

                        for (int indexPivot = 0; indexPivot < lastPivotElement; ++indexPivot) {
                            int indexPartner = indexPivot + windowDistance;
                            if (indexPartner >= partitionIndices.length) {
                                break;
                            }

                            detectDuplicates(records, partitionIndices, indexPivot, indexPartner);
                        }
                    }
                }
            }
        }
    }

    private void runWindowLinearIncreasingWithLookahead(int[] order) throws IOException {
        int localWindowInterval = this.partitionSize - this.windowSize + 1 <= order.length ? this.windowInterval : this.windowSize;
        HashMap<Integer, HashSet<Integer>> lookaheads = new HashMap<>();

        for (int i = 1; i < this.windowSize; ++i) {
            lookaheads.put(i, new HashSet<>());
        }

        for (int currentWindowStart = 1; currentWindowStart < this.windowSize; currentWindowStart += localWindowInterval) {
            int currentWindowEnd = Math.min(currentWindowStart + localWindowInterval, this.windowSize);
            if (currentWindowStart > this.windowInterval || localWindowInterval != this.windowInterval) {
                for (int partitionStartIndex = 0; partitionStartIndex < order.length; partitionStartIndex += this.partitionSize - this.windowSize + 1) {
                    int partitionEndIndex = Math.min(partitionStartIndex + this.partitionSize, order.length);
                    int[] partitionIndices = Arrays.copyOfRange(order, partitionStartIndex, partitionEndIndex);
                    HashMap<Integer, Record> records = this.dataReader.readLines(partitionIndices);
                    int lastPivotElement = partitionStartIndex + this.partitionSize - this.windowSize + 1 < order.length ? partitionIndices.length - this.windowSize + 1 : partitionIndices.length;

                    for (int windowDistance = currentWindowStart; windowDistance < currentWindowEnd; ++windowDistance) {
                        HashSet<Integer> currentLookaheads = lookaheads.get(windowDistance);

                        for (int indexPivot = 0; indexPivot < lastPivotElement; ++indexPivot) {
                            int indexPartner = indexPivot + windowDistance;
                            if (indexPartner >= partitionIndices.length) {
                                break;
                            }

                            if (!currentLookaheads.contains(partitionIndices[indexPivot])) {
                                int currentNumDuplicates = this.resultCollector.getDuplicates().size();
                                detectDuplicates(records, partitionIndices, indexPivot, indexPartner);

                                if (currentNumDuplicates == this.resultCollector.getDuplicates().size()) {
                                    this.lookAhead(indexPivot - 1, indexPartner, partitionIndices, records, lookaheads);
                                    this.lookAhead(indexPivot, indexPartner + 1, partitionIndices, records, lookaheads);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void lookAhead(int indexPivot, int indexPartner, int[] partitionIndices, HashMap<Integer, Record> records, HashMap<Integer, HashSet<Integer>> lookaheads) {
        int distance = indexPartner - indexPivot;
        if (distance + 1 <= this.windowSize) {
            if (!lookaheads.containsKey(distance)) {
                lookaheads.put(distance, new HashSet<>());
            }

            if (indexPivot >= 0 && indexPartner < partitionIndices.length && !(lookaheads.get(distance)).contains(partitionIndices[indexPivot])) {
                (lookaheads.get(distance)).add(partitionIndices[indexPivot]);
                detectDuplicates(records, partitionIndices, indexPivot, indexPartner);
            }
        }
    }

    private int runSingleWindowDistance(int[] order, int windowDistance) throws IOException {
        int numDuplicates = 0;

        for (int partitionStartIndex = 0; partitionStartIndex < order.length; partitionStartIndex += this.partitionSize - windowDistance) {
            int partitionEndIndex = Math.min(partitionStartIndex + this.partitionSize, order.length);
            int[] partitionIndices = Arrays.copyOfRange(order, partitionStartIndex, partitionEndIndex);
            HashMap<Integer, Record> records = this.dataReader.readLines(partitionIndices);
            int lastPivotElement = partitionIndices.length - windowDistance;

            for (int indexPivot = 0; indexPivot < lastPivotElement; ++indexPivot) {
                int indexPartner = indexPivot + windowDistance;
                detectDuplicates(records, partitionIndices, indexPivot, indexPartner);
            }
        }

        return numDuplicates;
    }

    private int findBestNextKey(int[] keysLastResultCount, int[] keysLastWindowSize) {
        int bestNextKey = -1;
        int largestLastResultCount = -1;

        for (int keyAttributeNumber = 0; keyAttributeNumber < keysLastResultCount.length; ++keyAttributeNumber) {
            if (largestLastResultCount < keysLastResultCount[keyAttributeNumber] && this.windowSize > keysLastWindowSize[keyAttributeNumber]) {
                largestLastResultCount = keysLastResultCount[keyAttributeNumber];
                bestNextKey = keyAttributeNumber;
            }
        }

        return bestNextKey;
    }
}
