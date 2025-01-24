package de.pdd_metadata.data_profiling;

import de.hpi.isg.sindy.util.PartialIND;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.binder.BINDERFile;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;;
import lombok.Getter;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class INDProfiler extends DependencyProfiler {
    private BINDERFile binder;
    private Set<InclusionDependency> inds = new HashSet<>();
    private Set<PartialIND> partialINDS = new HashSet<>();
    private DefaultFileInputGenerator fileInputGenerator;

    public INDProfiler(DefaultFileInputGenerator fileInputGenerator) {
        this.fileInputGenerator = fileInputGenerator;
        this.binder = new BINDERFile();
    }

    static class Parameters {
        private static final boolean NULL_EQUALS_NULL = true;
        private static final boolean VALIDATE_PARALLEL = true;
        private static final boolean ENABLE_MEMORY_GUARDIAN = true;
        private static final boolean DETECT_NARY = false;
        private static final int MAX_SEARCH_SPACE_LEVEL = -1;
        private static final int FILE_MAX_ROWS = -1;
    }

    public void executePartialINDProfiler() throws Exception {
    }

    public void executeFullINDProfiler() throws AlgorithmExecutionException, FileNotFoundException {
        RelationalInputGenerator[] inputs = new RelationalInputGenerator[]{fileInputGenerator, fileInputGenerator};

        List<ColumnIdentifier> columnIdentifiers = new ArrayList<>();
        columnIdentifiers.addAll(getAcceptedColumns(inputs[0]));
        columnIdentifiers.addAll(getAcceptedColumns(inputs[1]));

        ResultCache resultReceiver = new ResultCache("MetanomeMock", columnIdentifiers);

        binder.setRelationalInputConfigurationValue(BINDERFile.Identifier.INPUT_FILES.name(), inputs);
        binder.setIntegerConfigurationValue(BINDERFile.Identifier.MAX_NARY_LEVEL.name(), Parameters.MAX_SEARCH_SPACE_LEVEL);
        binder.setIntegerConfigurationValue(BINDERFile.Identifier.INPUT_ROW_LIMIT.name(), Parameters.FILE_MAX_ROWS);
        binder.setBooleanConfigurationValue(BINDERFile.Identifier.DETECT_NARY.name(), Parameters.DETECT_NARY);
        binder.setResultReceiver(resultReceiver);

        suppressSysOut(() -> {
            try {
                binder.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        List<Result> results = resultReceiver.fetchNewResults();
        inds = results.stream().map(x -> (InclusionDependency) x)
                .filter(x -> !x.getDependant().toString().equals(x.getReferenced().toString()))
                .collect(Collectors.toSet());
        inds.forEach(System.out::println);
    }
}
