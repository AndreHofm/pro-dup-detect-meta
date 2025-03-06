package de.uni_marburg.pdd_metadata.data_profiling;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeWeight;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class AttributeWeightProfiler {
    private FDProfiler fdProfiler;
    private INDProfiler indProfiler;
    private UCCProfiler uccProfiler;
    private DataReader dataReader;
    private List<AttributeWeight> attributeWeights;
    private String datasetName;
    private Logger log = LogManager.getLogger(AttributeWeightProfiler.class);

    public AttributeWeightProfiler(DataReader dataReader, String input, Configuration config) {
        this.dataReader = dataReader;

        DefaultFileInputGenerator fileInputGenerator = getInputGenerator(input);

        this.uccProfiler = new UCCProfiler(fileInputGenerator);
        this.fdProfiler = new FDProfiler(fileInputGenerator, dataReader);
        this.indProfiler = new INDProfiler(fileInputGenerator);
        this.attributeWeights = new ArrayList<>();
        this.datasetName = config.getDatasetName();
    }

    public void execute() throws Exception {
        this.log.info("Starting Attribute Weight Profiler...");
        var starTime = System.currentTimeMillis();
        initializeAttributeScoreList();

        Set<String> filterAttributesByNullValues = filterAttributesByNullValues();

        fdProfiler.executeFullFDProfiler();
        List<FunctionalDependency> fullFDs = fdProfiler.getFullFDs();

        uccProfiler.executeFullUCCProfiler();
        uccProfiler.executeKeyProfiler();
        Set<UniqueColumnCombination> keys = uccProfiler.getKeys();
        Set<UniqueColumnCombination> fullUCCs = uccProfiler.getFullUCCs();

        indProfiler.executePartialINDProfiler();
        indProfiler.executeFullINDProfiler();
        Set<InclusionDependency> partialINDs = indProfiler.getPartialINDS();
        Set<InclusionDependency> fullINDS = removeIdenticalINDs(indProfiler.getInds());
        partialINDs.addAll(fullINDS);

        Set<String> filteredKeys = filteringKeys(keys);

        Set<String> filteredINDs = filteringINDSs(partialINDs);

        Map<String, Long> filteredUCCs = filteringUCCs(fullUCCs, filterAttributesByNullValues, filteredKeys, filteredINDs);
        Map<String, Long> sortedFilteredUCCs =  sortDependencyMap(filteredUCCs);

        Set<String> dependants = getDependant(fullFDs, filterAttributesByNullValues, filteredKeys, filteredINDs).keySet();

        Map<String, Long> filteredFDs = filteringFDs(fullFDs, filterAttributesByNullValues, filteredKeys, filteredINDs);
        Map<String, Long> sortedFilteredFDs = sortDependencyMap(filteredFDs);

        this.log.info("Number of UCCs: {}", fullUCCs.size());
        this.log.info("Number of INDs: {}", partialINDs.size());
        this.log.info("FDs: {}", sortedFilteredFDs);
        this.log.info("Keys: {}", filteredKeys);
        this.log.info("UCCs: {}", sortedFilteredUCCs);
        this.log.info("INDs: {}", filteredINDs);

        this.attributeWeights.removeIf(attributeWeight -> filteredKeys.contains(attributeWeight.getAttribute())
                || !filterAttributesByNullValues.contains(attributeWeight.getAttribute())
                || filteredINDs.contains(attributeWeight.getAttribute())
                || (!new HashSet<>(filteredFDs.keySet()).contains(attributeWeight.getAttribute()) && new HashSet<>(dependants).contains(attributeWeight.getAttribute()))
                || attributeWeight.getAttribute().equals("dataset"));

        this.weightAttributes(sortedFilteredFDs, sortedFilteredUCCs);

        var endTime = System.currentTimeMillis() - starTime;
        this.log.info("Ending Attribute Weight Profiler - (Runtime: {}ms)", endTime);
    }

    private void weightAttributes(Map<String, Long> fds, Map<String, Long> uccs) {
        List<String> sortedFDAttributes = fds.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .toList();

        List<String> sortedUCCAttributes = uccs.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .toList();

        int sumFD = 0;
        for (Long value : fds.values()) {
            sumFD = sumFD + value.intValue();
        }

        int sumUCC = 0;
        for (Long value : uccs.values()) {
            sumUCC = sumUCC + value.intValue();
        }

        HashMap<String, Double> nameWeightFD = calculateAttributeDependencyWeight(fds, sortedFDAttributes, sumFD);
        ;

        HashMap<String, Double> nameWeightUCC = calculateAttributeDependencyWeight(uccs, sortedUCCAttributes, sumUCC);

        for (AttributeWeight attributeWeight : this.attributeWeights) {
            String attribute = attributeWeight.getAttribute();
            double weight = 0;
            if (nameWeightFD.containsKey(attribute) && nameWeightUCC.containsKey(attribute)) {
                weight = (nameWeightFD.get(attribute) + nameWeightUCC.get(attribute)) / 2.0;
            } else if (nameWeightFD.containsKey(attribute) && !nameWeightUCC.containsKey(attribute)) {
                weight = nameWeightFD.get(attribute) / (nameWeightUCC.isEmpty() ? 1.0 : 2.0);
            } else if (!nameWeightFD.containsKey(attribute) && nameWeightUCC.containsKey(attribute)) {
                weight = nameWeightUCC.get(attribute) / (nameWeightFD.isEmpty() ? 1.0 : 2.0);
            }

            attributeWeight.setWeight(weight);
        }
    }

    private HashMap<String, Double> calculateAttributeDependencyWeight(Map<String, Long> dependencies, List<String> sortedFDAttributes, int sumDependency) {
        HashMap<String, Double> nameWeightDependency = new HashMap<>();
        for (int i = 0; i < dependencies.size(); i++) {
            String name = sortedFDAttributes.get(i);
            double weight = (double) dependencies.get(name) / sumDependency;
            nameWeightDependency.put(name, weight);
        }

        return nameWeightDependency;
    }

    private Set<String> filterAttributesByNullValues() {
        int datasetSize = this.dataReader.getNumRecords();
        HashMap<String, Integer> attributesNull = this.dataReader.countNullValues();

        return attributesNull.entrySet().stream()
                .filter(x -> !(((double) x.getValue() / datasetSize) >= (datasetName.equals("census") ? 0.15 : 0.05)))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private Map<String, Long> filteringFDs(List<FunctionalDependency> fds, Set<String> filterAttributesByNullValues, Set<String> filteredUCCs, Set<String> filteredINDs) {
        return fds.stream()
                .flatMap(fd -> fd.getDeterminant().getColumnIdentifiers().stream())
                .map(ColumnIdentifier::getColumnIdentifier)
                .filter(filterAttributesByNullValues::contains)
                .filter(column -> !filteredINDs.contains(column) && !filteredUCCs.contains(column))
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
        /*
                .keySet().stream()
                .limit(fds.size())
                .collect(Collectors.toSet());

         */
    }

    private Set<String> filteringKeys(Set<UniqueColumnCombination> uccs) {
        return uccs.stream()
                .flatMap(x -> x.getColumnCombination().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""))
                .collect(Collectors.toSet());
    }

    private Map<String, Long> filteringUCCs(Set<UniqueColumnCombination> uccs, Set<String> filterAttributesByNullValues, Set<String> filteredKeys, Set<String> filteredINDs) {
        return uccs.stream()
                .flatMap(x -> x.getColumnCombination().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""))
                .filter(filterAttributesByNullValues::contains)
                .filter(column -> !filteredINDs.contains(column) && !filteredKeys.contains(column))
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
    }

    private Set<String> filteringINDSs(Set<InclusionDependency> inds) {
        return inds.stream()
                .flatMap(x -> x.getDependant().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""))
                .collect(Collectors.toSet());
    }

    private Map<String, Long> getDependant(List<FunctionalDependency> fds, Set<String> filterAttributesByNullValues, Set<String> filteredUCCs, Set<String> filteredINDs) {
        return fds.stream()
                .map(fd -> fd.getDependant().getColumnIdentifier())
                .filter(filterAttributesByNullValues::contains)
                .filter(column -> !filteredINDs.contains(column) && !filteredUCCs.contains(column))
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
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

    private Map<String, Long> sortDependencyMap(Map<String, Long> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(LinkedHashMap::new,
                        (m, e) -> m.put(e.getKey(), e.getValue()),
                        LinkedHashMap::putAll);
    }

    private void initializeAttributeScoreList() {
        String[] attributeNames = dataReader.getAttributeNames();

        for (int i = 0; i < attributeNames.length; i++) {
            AttributeWeight attribute = new AttributeWeight(i, attributeNames[i]);
            this.attributeWeights.add(attribute);
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
}
