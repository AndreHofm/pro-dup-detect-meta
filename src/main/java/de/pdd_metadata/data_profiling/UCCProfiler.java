package de.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.algorithms.Pyro;
import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.PartialFD;
import de.hpi.isg.pyro.model.PartialKey;
import de.hpi.isg.pyro.model.Vertical;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.algorithms.hyucc.HyUCC;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import lombok.Getter;
import org.apache.commons.io.output.NullOutputStream;

import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class UCCProfiler extends DependencyProfiler {
    private final Pyro pyro = new Pyro();
    private final HyUCC hyUCC = new HyUCC();
    private DefaultFileInputGenerator fileInputGenerator;
    private List<PartialKey> partialUCCs = new ArrayList<>();
    private Set<UniqueColumnCombination> fullUCCs = new HashSet<>();

    public UCCProfiler(DefaultFileInputGenerator fileInputGenerator) {
        this.fileInputGenerator = fileInputGenerator;
    }

    static class Parameters {
        private static final boolean NULL_EQUALS_NULL = true;
        private static final boolean VALIDATE_PARALLEL = true;
        private static final boolean ENABLE_MEMORY_GUARDIAN = true;
        private static final boolean DETECT_NARY = true;
        private static final int MAX_SEARCH_SPACE_LEVEL = -1;
        private static final int FILE_MAX_ROWS = -1;
    }

    public HashMap<Vertical, Long> executePartialUCCProfiler() throws Exception {
        pyro.setRelationalInputConfigurationValue("inputFile", fileInputGenerator);
        pyro.setBooleanConfigurationValue("isNullEqualNull", true);
        pyro.setBooleanConfigurationValue("isFindFds", false);

        pyro.setUccConsumer(partialUCCs::add);

        suppressSysOut(() -> {
            try {
                pyro.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        var test = partialUCCs.stream()
                .flatMap(ucc -> Stream.of(ucc.vertical.getColumns()))
                .collect(Collectors.groupingBy(Column::getName, Collectors.counting()));

        System.out.println(test);

        return (HashMap<Vertical, Long>) partialUCCs.stream().collect(Collectors.groupingBy(key -> key.vertical, Collectors.counting()));
    }

    public void executeFullUCCProfiler() throws Exception {
        hyUCC.setRelationalInputConfigurationValue(HyUCC.Identifier.INPUT_GENERATOR.name(), fileInputGenerator);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.NULL_EQUALS_NULL.name(), Parameters.NULL_EQUALS_NULL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.VALIDATE_PARALLEL.name(), Parameters.VALIDATE_PARALLEL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.ENABLE_MEMORY_GUARDIAN.name(), Parameters.ENABLE_MEMORY_GUARDIAN);
        hyUCC.setIntegerConfigurationValue(HyUCC.Identifier.MAX_UCC_SIZE.name(), Parameters.MAX_SEARCH_SPACE_LEVEL);
        hyUCC.setIntegerConfigurationValue(HyUCC.Identifier.INPUT_ROW_LIMIT.name(), Parameters.FILE_MAX_ROWS);

        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(fileInputGenerator));

        hyUCC.setResultReceiver(resultReceiver);

        suppressSysOut(() -> {
            try {
                hyUCC.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        List<Result> results = resultReceiver.fetchNewResults();

        fullUCCs = results.stream().map(x -> (UniqueColumnCombination) x).collect(Collectors.toSet());
    }
}
