package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.io.DataWriter;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class ResultCollector {
    private Configuration config;
    private DataReader dataReader;
    private DataWriter dataWriter;
    @Setter
    private long startTime;
    private final Logger log = LogManager.getLogger(ResultCollector.class);
    private long lastComparisonMeasurement;
    private Set<Duplicate> duplicates = new HashSet<>();
    private Set<Duplicate> goldResults = new HashSet<>();
    private double precision;
    private double recall;
    private double f1;
    private LinkedBlockingQueue<Triple<Long, Integer, Integer>> duplicateMeasurements;
    private LinkedBlockingQueue<Integer> comparisonMeasurements = new LinkedBlockingQueue<>();

    public ResultCollector(DataReader dataReader, DataWriter dataWriter, Configuration config) {
        this.config = config;
        this.dataReader = dataReader;
        this.dataWriter = dataWriter;
        this.startTime = System.currentTimeMillis();
        this.lastComparisonMeasurement = this.startTime;
    }

    public ResultCollector(DataReader dataReader, Configuration config) {
        this.config = config;
        this.dataReader = dataReader;
        this.dataWriter = null;
        this.startTime = System.currentTimeMillis();
        this.lastComparisonMeasurement = this.startTime;
    }

    public void collectDuplicate(Record record1, Record record2) {
        Duplicate duplicate = new Duplicate(record1.index, record2.index, record1.values[0], record2.values[0], System.currentTimeMillis() - this.startTime);

        boolean newDuplicate = !this.duplicates.contains(duplicate);

        if (newDuplicate) {
            this.duplicates.add(duplicate);
        }
    }

    public void collectGoldResult() {
        Set<String> sampleIds = dataReader.readResultDuplicatesSamples();

        this.goldResults = dataReader.readResultDuplicates(sampleIds);
    }

    public void logResults() {
        collectGoldResult();

        Set<Duplicate> results = this.getDuplicates();

        Set<Duplicate> fn = new HashSet<>(goldResults);
        fn.removeAll(results);

        Set<Duplicate> fp = new HashSet<>(results);
        fp.removeAll(goldResults);

        int tpSize = results.size() - fp.size();
        int fpSize = fp.size();
        int fnSize = fn.size();

        this.precision = (double) tpSize / (double) (tpSize + fpSize);
        this.recall = (double) tpSize / (double) (tpSize + fnSize);
        this.f1 = (double) (2 * tpSize) / (double) (2 * tpSize + fnSize + fpSize);

        log.info("Number of Duplicates: {}", this.getDuplicates().size());
        log.info("Number of actual Duplicates: {}", this.goldResults.size());
        log.info("Precession: {}", this.precision);
        log.info("Recall: {}", this.recall);
        log.info("F1-Score: {}", this.f1);


        calculateDuplicateMeasurements();
        float integralQuali = this.calculateIntegralQuality(this.duplicateMeasurements,299, config.getQualityTimeInMs());
        log.info("Integral Quality: {}", integralQuali);
    }

    public void logTimestemp(){
        System.out.println(this.getDuplicates().stream().filter(duplicate -> duplicate.getTimestamp() > 500L).toList().size());
    }

    private void calculateDuplicateMeasurements() {
        this.duplicateMeasurements = new LinkedBlockingQueue<>();
        List<Duplicate> sortedDuplicates = new ArrayList<>(this.duplicates);
        Collections.sort(sortedDuplicates);
        LinkedBlockingQueue<Integer> tempComparisonMeasurements = clone(this.comparisonMeasurements);
        long measurementIntervalLength = this.config.getResultMeasurementIntervalInMs();
        long measurementInterval = 1L;
        int numDuplicates = 0;

        for(Iterator<Duplicate> var9 = sortedDuplicates.iterator(); var9.hasNext(); ++numDuplicates) {
            for(Duplicate duplicate = var9.next(); duplicate.getTimestamp() > measurementInterval * measurementIntervalLength; ++measurementInterval) {
                this.duplicateMeasurements.add(new ImmutableTriple<>(measurementInterval * measurementIntervalLength, numDuplicates, tempComparisonMeasurements.poll()));
            }
        }

        while(!tempComparisonMeasurements.isEmpty()) {
            this.duplicateMeasurements.add(new ImmutableTriple<>(measurementInterval * measurementIntervalLength, numDuplicates, tempComparisonMeasurements.poll()));
            ++measurementInterval;
        }

    }

    private float calculateIntegralQuality(LinkedBlockingQueue<Triple<Long, Integer, Integer>> duplicateMeasurements, int numDuplicatesGold, long endTime) {
        float integral = 0.0F;
        Long previousTime = 0L;
        Integer previousCount = 0;

        for(Triple<Long, Integer, Integer> duplicateMeasurement : duplicateMeasurements) {
            if (duplicateMeasurement.getLeft() > endTime) {
                break;
            }

            integral += (float)(duplicateMeasurement.getLeft() - previousTime) * ((float)previousCount / (float)numDuplicatesGold);
            previousTime = duplicateMeasurement.getLeft();
            previousCount = duplicateMeasurement.getMiddle();
        }

        integral += (float)(endTime - previousTime) * ((float)previousCount / (float)numDuplicatesGold);
        return integral / (float)endTime;
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

    public void clearDuplicates() {
        this.duplicates.clear();
    }
}
