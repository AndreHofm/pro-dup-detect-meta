package de.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.model.Vertical;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.pdd_metadata.data_profiling.structures.AttributeScore;
import de.pdd_metadata.io.DataReader;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class AttributeScoringProfiler {
    private FDProfiler fdProfiler;
    private INDProfiler indProfiler;
    private UCCProfiler uccProfiler;
    private DataReader dataReader;
    private List<AttributeScore> attributeScores;

    public AttributeScoringProfiler(DataReader dataReader, DefaultFileInputGenerator fileInputGenerator) {
        this.dataReader = dataReader;
        this.uccProfiler = new UCCProfiler(fileInputGenerator);
        this.fdProfiler = new FDProfiler(fileInputGenerator);
        this.indProfiler = new INDProfiler(fileInputGenerator);
        this.attributeScores = new ArrayList<>();
    }

    public void execute() throws Exception {
        initializeAttributeScoreList();

        Set<String> filterAttributesByNullValues = filterAttributesByNullValues();

        /*
        System.out.println("Starte uccProfiler...");
        HashMap<Vertical, Long> numberAttributePartialUCC = uccProfiler.executePartialUCCProfiler();
        System.out.println("uccProfiler fertig!");

        System.out.println("Starte fdProfiler...");
        HashMap<Vertical, Long> numberAttributePartialFD = fdProfiler.executePartialFDProfiler();
        System.out.println("fdProfiler fertig!");

         */



        // fdProfiler.executeFullFDProfiler(filterAttributesByNullValues);

        // uccProfiler.executeFullUCCProfiler();

        indProfiler.executeFullINDProfiler();

        Set<UniqueColumnCombination> fullUCCs = uccProfiler.getFullUCCs();

        Set<String> filteredUCCs = fullUCCs.stream()
                .flatMap(x -> x.getColumnCombination().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace("cd.csv.", "")).collect(Collectors.toSet());

        this.attributeScores.removeIf(attributeScore -> filteredUCCs.contains(attributeScore.getAttribute()) || !filterAttributesByNullValues.contains(attributeScore.getAttribute()));

        //System.out.println(numberAttributePartialUCC);
        //System.out.println(numberAttributePartialFD);

        // Currently Pyro doesnt stop all threads?!
        //System.exit(0);
    }

    private void initializeAttributeScoreList() {
        String[] attributeNames = dataReader.getAttributeNames();

        for (int i = 0; i < attributeNames.length; i++) {
            AttributeScore attribute = new AttributeScore(i, attributeNames[i]);
            this.attributeScores.add(attribute);
        }
    }

    public Set<String> filterAttributesByNullValues() {
        HashMap<String, Integer> attributesNull = this.dataReader.countNullValues();

        return attributesNull.entrySet().stream()
                .filter(x -> !(((double) x.getValue() / 9763) >= 0.01))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
}
