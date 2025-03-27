package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.io.EvalWriter;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class ResultCollector {
    private final Configuration config;
    private final DataReader dataReader;
    @Setter
    private long startTime;
    private final Logger log = LogManager.getLogger(ResultCollector.class);
    private final long lastComparisonMeasurement;
    private final Set<Duplicate> duplicates = new HashSet<>();
    private Set<Duplicate> goldResults = new HashSet<>();
    private double precision;
    private double recall;
    private double f1;
    private LinkedBlockingQueue<Triple<Long, Integer, Integer>> duplicateMeasurements;
    private final LinkedBlockingQueue<Integer> comparisonMeasurements = new LinkedBlockingQueue<>();


    public ResultCollector(DataReader dataReader, Configuration config) {
        this.config = config;
        this.dataReader = dataReader;
        this.startTime = System.currentTimeMillis();
        this.lastComparisonMeasurement = this.startTime;
    }

    public void collectDuplicate(Record record1, Record record2) {
        Duplicate duplicate = new Duplicate(record1.index, record2.index, record1.values[0], record2.values[0], System.currentTimeMillis() - this.startTime);

        this.duplicates.add(duplicate);
    }

    public void collectGoldResult() {
        Set<String> sampleIds = dataReader.readResultDuplicatesSamples();

        this.goldResults = dataReader.readResultDuplicates(sampleIds);
    }

    public void logResults() {
        collectGoldResult();

        Set<Duplicate> fn = new HashSet<>(goldResults);
        fn.removeAll(duplicates);

        Set<Duplicate> fp = new HashSet<>(duplicates);
        fp.removeAll(goldResults);

        Set<Duplicate> tp = new HashSet<>(duplicates);
        tp.removeAll(fp);

        int tpSize = tp.size();
        int fpSize = fp.size();
        int fnSize = fn.size();

        this.precision = (double) tpSize / (double) (tpSize + fpSize);
        this.recall = (double) tpSize / (double) (tpSize + fnSize);
        this.f1 = (double) (2 * tpSize) / (double) (2 * tpSize + fnSize + fpSize);

        log.info("Number of Duplicates: {}", duplicates.size());
        log.info("Number of actual Duplicates: {}", goldResults.size());
        log.info("Number of True Positives: {}", tpSize);
        log.info("Precession: {}", this.precision);
        log.info("Recall: {}", this.recall);
        log.info("F1-Score: {}", this.f1);


        calculateDuplicateMeasurements(tp);
        float integralQuali = this.calculateIntegralQuality(this.duplicateMeasurements, goldResults.size(), config.getQualityTimeInMs());
        log.info("Integral Quality: {}", integralQuali);

        if (config.isWRITE_INTEGRAL_QUALI()) {
            String resultPath = "./results/" + config.getALGORITHM() + "/" + config.getDatasetName() + "/progressive_quality_measure_with_pf/";

            EvalWriter.writeDuplicatesPerTime(resultPath + "duplicates_per_time", this.duplicateMeasurements, config.getQualityTimeInMs(), config);
        }
    }

    private void calculateDuplicateMeasurements(Set<Duplicate> tp) {
        this.duplicateMeasurements = new LinkedBlockingQueue<>();
        List<Duplicate> sortedDuplicates = new ArrayList<>(tp);
        Collections.sort(sortedDuplicates);
        LinkedBlockingQueue<Integer> tempComparisonMeasurements = clone(this.comparisonMeasurements);
        long measurementIntervalLength = this.config.getResultMeasurementIntervalInMs();
        long measurementInterval = 1L;
        int numDuplicates = 0;

        for (Iterator<Duplicate> duplicateIterator = sortedDuplicates.iterator(); duplicateIterator.hasNext(); ++numDuplicates) {
            for (Duplicate duplicate = duplicateIterator.next(); duplicate.getTimestamp() > measurementInterval * measurementIntervalLength; ++measurementInterval) {
                this.duplicateMeasurements.add(new ImmutableTriple<>(measurementInterval * measurementIntervalLength, numDuplicates, tempComparisonMeasurements.poll()));
            }
        }

        while (!tempComparisonMeasurements.isEmpty()) {
            this.duplicateMeasurements.add(new ImmutableTriple<>(measurementInterval * measurementIntervalLength, numDuplicates, tempComparisonMeasurements.poll()));
            ++measurementInterval;
        }

    }

    private float calculateIntegralQuality(LinkedBlockingQueue<Triple<Long, Integer, Integer>> duplicateMeasurements, int numDuplicatesGold, long endTime) {
        float integral = 0.0F;
        Long previousTime = 0L;
        Integer previousCount = 0;

        for (Triple<Long, Integer, Integer> duplicateMeasurement : duplicateMeasurements) {
            if (duplicateMeasurement.getLeft() > endTime) {
                break;
            }

            integral += (float) (duplicateMeasurement.getLeft() - previousTime) * ((float) previousCount / (float) numDuplicatesGold);
            previousTime = duplicateMeasurement.getLeft();
            previousCount = duplicateMeasurement.getMiddle();
        }

        integral += (float) (endTime - previousTime) * ((float) previousCount / (float) numDuplicatesGold);
        return integral / (float) endTime;
    }

    public static LinkedBlockingQueue<Integer> clone(LinkedBlockingQueue<Integer> queue) {
        if (queue.isEmpty()) {
            return new LinkedBlockingQueue<>();
        } else {
            LinkedBlockingQueue<Integer> clone = new LinkedBlockingQueue<>(queue.size());

            clone.addAll(queue);

            return clone;
        }
    }
}
