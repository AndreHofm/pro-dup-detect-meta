package de.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.algorithms.Pyro;
import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.PartialFD;
import de.hpi.isg.pyro.model.Vertical;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.HyFD;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class FDProfiler extends DependencyProfiler {
    private final Pyro pyro = new Pyro();
    private final HyFD hyFD = new HyFD();
    private DefaultFileInputGenerator fileInputGenerator;
    private List<PartialFD> partialFDs = new ArrayList<>();
    private Set<String> fullFDs = new HashSet<>();

    public FDProfiler(DefaultFileInputGenerator fileInputGenerator) {
        this.fileInputGenerator = fileInputGenerator;
    }

    static class Parameters {
        private static final boolean NULL_EQUALS_NULL = true;
        private static final boolean VALIDATE_PARALLEL = true;
        private static final boolean ENABLE_MEMORY_GUARDIAN = true;
        private static final int MAX_SEARCH_SPACE_LEVEL = 2;
        private static final int FILE_MAX_ROWS = -1;
    }

    public HashMap<Vertical, Long> executePartialFDProfiler() throws Exception {
        pyro.setRelationalInputConfigurationValue("inputFile", fileInputGenerator);
        pyro.setBooleanConfigurationValue("isNullEqualNull", true);
        pyro.setBooleanConfigurationValue("isFindKeys", false);

        pyro.setFdConsumer(partialFDs::add);

        suppressSysOut(() -> {
            try {
                pyro.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        var test = partialFDs.stream()
                .flatMap(fd -> Stream.of(fd.rhs.getColumns()))
                .collect(Collectors.groupingBy(Column::getName, Collectors.counting()));

        System.out.println(test);

        return (HashMap<Vertical, Long>) partialFDs.stream().collect(Collectors.groupingBy(key -> key.lhs, Collectors.counting()));
    }

    public void executeFullFDProfiler(Set<String> filteredAttributes) throws Exception {
        hyFD.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), fileInputGenerator);
        hyFD.setBooleanConfigurationValue(HyFD.Identifier.NULL_EQUALS_NULL.name(), Parameters.NULL_EQUALS_NULL);
        hyFD.setBooleanConfigurationValue(HyFD.Identifier.VALIDATE_PARALLEL.name(), Parameters.VALIDATE_PARALLEL);
        hyFD.setBooleanConfigurationValue(HyFD.Identifier.ENABLE_MEMORY_GUARDIAN.name(), Parameters.ENABLE_MEMORY_GUARDIAN);
        // hyFD.setIntegerConfigurationValue(HyFD.Identifier.MAX_DETERMINANT_SIZE.name(), Parameters.MAX_SEARCH_SPACE_LEVEL);
        hyFD.setIntegerConfigurationValue(HyFD.Identifier.INPUT_ROW_LIMIT.name(), Parameters.FILE_MAX_ROWS);

        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(fileInputGenerator));

        hyFD.setResultReceiver(resultReceiver);

        suppressSysOut(() -> {
            try {
                hyFD.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        List<Result> results = resultReceiver.fetchNewResults();

        var test = results.stream().map(x -> (FunctionalDependency) x)
                // .filter(fd -> filteredAttributes.contains(fd.getDependant().getColumnIdentifier()))
                .flatMap(fd -> fd.getDeterminant().getColumnIdentifiers().stream())
                .map(ColumnIdentifier::getColumnIdentifier)
                .map(x -> x.replace("Amazon_Google.csv.", ""))
                .filter(filteredAttributes::contains)
                //.collect(Collectors.toSet());
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

        // fullFDs = test;

        var test3 = new ArrayList<>(test.entrySet().stream().toList());

        System.out.println(test3);

        test3.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        fullFDs = test3.stream()
                .map(Map.Entry::getKey)
                .toList().stream()
                //.limit(7)
                .collect(Collectors.toSet());

        System.out.println(fullFDs);
    }
}
