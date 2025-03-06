package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.io.DataWriter;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

@Getter
public class ResultCollector {
    private Configuration config;
    private DataReader dataReader;
    private DataWriter dataWriter;
    private long startTime;
    private final Logger log = LogManager.getLogger(ResultCollector.class);
    private long lastComparisonMeasurement;
    private Set<Duplicate> duplicates = new HashSet<>();
    private Set<Duplicate> goldResults = new HashSet<>();

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

        log.info("Number of Duplicates: {}", this.getDuplicates().size());
        log.info("Number of actual Duplicates: {}", this.goldResults.size());
        log.info("True Positive: {}", tpSize);
        log.info("False Positive: {}", fpSize);
        log.info("False Negative: {}", fnSize);
        log.info("Precession: {}", (double) tpSize / (double) (tpSize + fpSize));
        log.info("Recall: {}", (double) tpSize / (double) (tpSize + fnSize));
        log.info("F1-Score: {}", (double) (2 * tpSize) / (double) (2 * tpSize + fnSize + fpSize));
    }
}
