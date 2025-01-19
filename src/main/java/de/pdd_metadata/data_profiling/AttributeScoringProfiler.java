package de.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.model.Vertical;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.pdd_metadata.data_profiling.structures.AttributeScore;
import de.pdd_metadata.io.DataReader;
import lombok.Getter;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Getter
public class AttributeScoringProfiler {
    private FDProfiler fdProfiler;
    // private INDProfiler indProfiler;
    private UCCProfiler uccProfiler;
    private DataReader dataReader;
    private List<AttributeScore> attributeScores;

    public AttributeScoringProfiler(DataReader dataReader, FileInputGenerator fileInputGenerator) {
        this.dataReader = dataReader;
        this.uccProfiler = new UCCProfiler(fileInputGenerator);
        this.fdProfiler = new FDProfiler(fileInputGenerator);
        this.attributeScores = new ArrayList<>();
    }

    public void execute() throws Exception {
        //initializeAttributeScoreList();

        System.out.println(attributeScores);


        System.out.println("Starte uccProfiler...");
        HashMap<Vertical, Long> numberAttributePartialUCC = uccProfiler.executePartialUCCProfiler();
        System.out.println("uccProfiler fertig!");

        System.out.println("Starte fdProfiler...");
        HashMap<Vertical, Long> numberAttributePartialFD = fdProfiler.executePartialFDProfiler();
        System.out.println("fdProfiler fertig!");

        System.out.println(numberAttributePartialUCC);
        System.out.println(numberAttributePartialFD);
    }

    private void initializeAttributeScoreList() {
        String[] attributeNames = dataReader.getAttributeNames();

        for (int i = 0; i < attributeNames.length; i++) {
            AttributeScore attribute = new AttributeScore(i, attributeNames[i]);
            this.attributeScores.add(attribute);
        }
    }

    public void filterAttributesByNullValues() {
        HashMap<String, Integer> attributesNull = this.dataReader.countNullValues();

        attributesNull.entrySet().stream().filter(x -> !(((double) x.getValue() / 9763) >= 0.01)).forEach(System.out::println);
    }
}
