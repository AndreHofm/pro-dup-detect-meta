package de.uni_marburg.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.algorithms.Pyro;
import de.hpi.isg.pyro.model.PartialKey;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.algorithms.hyucc.HyUCC;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class UCCProfiler extends DependencyProfiler {
    private final Pyro pyro = new Pyro();
    private final HyUCC hyUCC = new HyUCC();
    private DefaultFileInputGenerator fileInputGenerator;
    private Set<UniqueColumnCombination> partialKeys = new HashSet<>();
    private Set<UniqueColumnCombination> fullUCCs = new HashSet<>();
    private Set<UniqueColumnCombination> keys = new HashSet<>();
    private Logger log = LogManager.getLogger(UCCProfiler.class);
    private Configuration config;

    public UCCProfiler(DefaultFileInputGenerator fileInputGenerator, Configuration config) {
        this.fileInputGenerator = fileInputGenerator;
        this.config = config;
    }

    static class Parameters {
        private static final boolean NULL_EQUALS_NULL = true;
        private static final boolean VALIDATE_PARALLEL = true;
        private static final boolean ENABLE_MEMORY_GUARDIAN = true;
        private static final int FILE_MAX_ROWS = -1;
    }

    public void executePartialUCCProfiler() throws Exception {
        pyro.setRelationalInputConfigurationValue("inputFile", fileInputGenerator);
        pyro.setBooleanConfigurationValue("isNullEqualNull", true);
        pyro.setBooleanConfigurationValue("isFindFds", false);
        pyro.setIntegerConfigurationValue("maxCols", 2);

        List<PartialKey> partialUCCs = new ArrayList<>();

        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(fileInputGenerator));
        pyro.setResultReceiver((UniqueColumnCombinationResultReceiver) resultReceiver);

        pyro.setUccConsumer(partialUCCs::add);

        suppressSysOut(() -> {
            try {
                pyro.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        partialKeys = partialUCCs.stream()
                .map(PartialKey::toMetanomeUniqueColumnCobination)
                .filter(keys -> keys.getColumnCombination().getColumnIdentifiers().size() == 1)
                .collect(Collectors.toSet());
    }

    public void executeFullUCCProfiler() throws Exception {
        hyUCC.setRelationalInputConfigurationValue(HyUCC.Identifier.INPUT_GENERATOR.name(), fileInputGenerator);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.NULL_EQUALS_NULL.name(), Parameters.NULL_EQUALS_NULL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.VALIDATE_PARALLEL.name(), Parameters.VALIDATE_PARALLEL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.ENABLE_MEMORY_GUARDIAN.name(), Parameters.ENABLE_MEMORY_GUARDIAN);
        hyUCC.setIntegerConfigurationValue(HyUCC.Identifier.MAX_UCC_SIZE.name(), config.getMaxUCCDeterminant());
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
        fullUCCs = results.stream().map(x -> (UniqueColumnCombination) x).filter(x -> x.getColumnCombination().getColumnIdentifiers().size() > 1).collect(Collectors.toSet());
    }

    public void executeKeyProfiler() throws Exception {
        hyUCC.setRelationalInputConfigurationValue(HyUCC.Identifier.INPUT_GENERATOR.name(), fileInputGenerator);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.NULL_EQUALS_NULL.name(), Parameters.NULL_EQUALS_NULL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.VALIDATE_PARALLEL.name(), Parameters.VALIDATE_PARALLEL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.ENABLE_MEMORY_GUARDIAN.name(), Parameters.ENABLE_MEMORY_GUARDIAN);
        hyUCC.setIntegerConfigurationValue(HyUCC.Identifier.MAX_UCC_SIZE.name(), 1);
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
        keys = results.stream().map(x -> (UniqueColumnCombination) x).collect(Collectors.toSet());

    }
}
