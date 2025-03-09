package de.uni_marburg.pdd_metadata.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class EvalWriter {
    final static Logger log = LogManager.getLogger(EvalWriter.class);

    public static void writeCSV(String filePath, List<Double> threshold, List<Double> precision, List<Double> recall, List<Double> f1Score, List<Integer> numberOfAttributes) {
        writeCSV(filePath, threshold, precision, "precision");
        writeCSV(filePath, threshold, recall, "recall");
        writeCSV(filePath, threshold, f1Score, "f1_score");
        writeCSVNumberOfAttributes(filePath, threshold, numberOfAttributes);
    }

    private static void writeCSV(String filePath, List<Double> threshold, List<Double> metric, String metricName) {
        String output = filePath + metricName + ".csv";
        Path parentDir = Path.of(output).getParent();

        try {
            if (parentDir != null) {
                Files.createDirectories(parentDir); // Erstellt das Verzeichnis, falls es nicht existiert
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
                String header = "Threshold," + metricName;
                writer.write(header);
                writer.newLine();

                for (int i = 0; i < threshold.size(); i++) {
                    writer.write(threshold.get(i) + "," + metric.get(i));
                    writer.newLine();
                }
                log.info("CSV-Datei wurde erfolgreich erstellt: {}", output);
            }
        } catch (IOException e) {
            log.error("Fehler beim Schreiben der CSV-Datei: {}", e.getMessage());
        }
    }

    private static void writeCSVNumberOfAttributes(String filePath, List<Double> threshold, List<Integer> numberOfAttributes) {
        String output = filePath + "number_of_attributes" + ".csv";
        Path parentDir = Path.of(output).getParent();

        try {
            if (parentDir != null) {
                Files.createDirectories(parentDir); // Erstellt das Verzeichnis, falls es nicht existiert
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
                String header = "Threshold," + "number_of_attributes";
                writer.write(header);
                writer.newLine();

                for (int i = 0; i < threshold.size(); i++) {
                    writer.write(threshold.get(i) + "," + numberOfAttributes.get(i));
                    writer.newLine();
                }
                log.info("CSV-Datei wurde erfolgreich erstellt: {}", output);
            }
        } catch (IOException e) {
            log.error("Fehler beim Schreiben der CSV-Datei: {}", e.getMessage());
        }
    }
}
