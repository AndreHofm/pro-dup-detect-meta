package de.uni_marburg.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.model.Vertical;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeScore;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.utils.Configuration;
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
    private String datasetName;

    public AttributeScoringProfiler(DataReader dataReader, String input, Configuration config) {
        this.dataReader = dataReader;

        DefaultFileInputGenerator fileInputGenerator = getInputGenerator(input);

        this.uccProfiler = new UCCProfiler(fileInputGenerator);
        this.fdProfiler = new FDProfiler(fileInputGenerator);
        this.indProfiler = new INDProfiler(fileInputGenerator);
        this.attributeScores = new ArrayList<>();
        this.datasetName = config.getDatasetName();
    }

    public void execute() throws Exception {
        initializeAttributeScoreList();

        Set<String> filterAttributesByNullValues = filterAttributesByNullValues();

        System.out.println("Null Values: " + filterAttributesByNullValues);

        /*
        System.out.println("Starte uccProfiler...");
        HashMap<Vertical, Long> numberAttributePartialUCC = uccProfiler.executePartialUCCProfiler();
        System.out.println("uccProfiler fertig!");

        System.out.println("Starte fdProfiler...");
        HashMap<Vertical, Long> numberAttributePartialFD = fdProfiler.executePartialFDProfiler();
        System.out.println("fdProfiler fertig!");

         */

        fdProfiler.executeFullFDProfiler();
        Set<FunctionalDependency> fullFDs = fdProfiler.getFullFDs();

        uccProfiler.executeFullUCCProfiler();
        Set<UniqueColumnCombination> fullUCCs = uccProfiler.getFullUCCs();

        indProfiler.executePartialINDProfiler();
        indProfiler.executeFullINDProfiler();
        Set<InclusionDependency> partialINDs = indProfiler.getPartialINDS();
        Set<InclusionDependency> fullINDS = removeIdenticalINDs(indProfiler.getInds());
        partialINDs.addAll(fullINDS);

        Set<String> filteredFDs = filteringFDs(fullFDs);

        Set<String> filteredUCCs = filteringUCCs(fullUCCs);

        Set<String> filteredINDs = filteringINDSs(partialINDs);

        System.out.println("FDs: " + filteredFDs);
        System.out.println("UCCs: " + filteredUCCs);
        System.out.println("INDs: " + filteredINDs);

        this.attributeScores.removeIf(attributeScore -> filteredUCCs.contains(attributeScore.getAttribute())
                || !filterAttributesByNullValues.contains(attributeScore.getAttribute())
                || filteredINDs.contains(attributeScore.getAttribute())
                || !filteredFDs.contains(attributeScore.getAttribute())
                || attributeScore.getAttribute().equals("dataset"));
    }

    private Set<String> filteringFDs(Set<FunctionalDependency> fds) {
        return fds.stream()
                .flatMap(fd -> fd.getDeterminant().getColumnIdentifiers().stream())
                .map(ColumnIdentifier::getColumnIdentifier)
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()))
                .keySet().stream()
                .limit(fds.size())
                .collect(Collectors.toSet());
    }

    private Set<String> filteringUCCs(Set<UniqueColumnCombination> uccs) {
        return uccs.stream()
                .flatMap(x -> x.getColumnCombination().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""))
                .collect(Collectors.toSet());
    }

    private Set<String> filteringINDSs(Set<InclusionDependency> inds) {
        return inds.stream()
                .flatMap(x -> x.getDependant().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""))
                .collect(Collectors.toSet());
    }

    private Set<InclusionDependency> removeIdenticalINDs(Set<InclusionDependency> inds) {
        List<InclusionDependency> list = new ArrayList<>(inds.stream().toList());

        for (int i = 0; i < list.size(); i++) {
            for (int j = 0; j < list.size(); j++) {
                if (list.get(i).getDependant().equals(list.get(j).getReferenced()) && list.get(j).getDependant().equals(list.get(i).getReferenced())) {
                    list.remove(list.get(j));
                }
            }
        }

        return new HashSet<>(list);
    }

    private void initializeAttributeScoreList() {
        String[] attributeNames = dataReader.getAttributeNames();

        for (int i = 0; i < attributeNames.length; i++) {
            AttributeScore attribute = new AttributeScore(i, attributeNames[i]);
            this.attributeScores.add(attribute);
        }
    }

    private static DefaultFileInputGenerator getInputGenerator(String path) {
        try {
            return new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                    path,
                    true,
                    ';',
                    '\"',
                    '\\',
                    false,
                    true,
                    0,
                    true,
                    true,
                    ""
            ));
        } catch (AlgorithmConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> filterAttributesByNullValues() {
        int datasetSize = this.dataReader.getNumRecords();
        HashMap<String, Integer> attributesNull = this.dataReader.countNullValues();

        return attributesNull.entrySet().stream()
                .filter(x -> !(((double) x.getValue() / datasetSize) >= 0.05))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
}
