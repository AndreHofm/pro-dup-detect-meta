package de.pdd_metadata.data_profiling;

import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Sandy;
import de.hpi.isg.sindy.util.PartialIND;
import de.hpi.mpss2015n.approxind.FAIDA;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.binder.BINDERFile;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import lombok.Getter;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class INDProfiler extends DependencyProfiler {
    // private Sindy sindy;
    private Sandy sandy;
    private BINDERFile binder;
    private FAIDA faida;
    private Set<InclusionDependency> inds = new HashSet<>();
    private Set<PartialIND> partialINDS = new HashSet<>();
    private DefaultFileInputGenerator fileInputGenerator;

    public INDProfiler(DefaultFileInputGenerator fileInputGenerator) {
        this.fileInputGenerator = fileInputGenerator;
        // this.faida = new FAIDA();
        int numColumnBits = 16;

        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList("file:" + "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/data/persons.tsv","file:" + "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/data/planets.tsv"),
                numColumnBits
        );
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createLocalEnvironment();

        executionEnvironment.setParallelism(1);
        this.sandy = new Sandy(
                indexedInputFiles,
                numColumnBits,
                executionEnvironment,
                partialINDS::add
        );

        this.binder = new BINDERFile();
    }

    static class Parameters {
        private static final boolean NULL_EQUALS_NULL = true;
        private static final boolean VALIDATE_PARALLEL = true;
        private static final boolean ENABLE_MEMORY_GUARDIAN = true;
        private static final boolean DETECT_NARY = true;
        private static final int MAX_SEARCH_SPACE_LEVEL = -1;
        private static final int FILE_MAX_ROWS = -1;
    }

    public void executePartialINDProfiler() throws Exception {
        sandy.setDropNulls(false);
        sandy.setFieldSeparator('\t');
        sandy.setMaxArity(1);
        sandy.setMinRelativeOverlap(0.0);

        sandy.run();

        System.out.println(sandy.getAllInds());
    }

    public void executeFullINDProfiler() throws AlgorithmExecutionException, FileNotFoundException {
        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(fileInputGenerator));

        binder.setRelationalInputConfigurationValue(BINDERFile.Identifier.INPUT_FILES.name(), fileInputGenerator);
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
        inds = results.stream().map(x -> (InclusionDependency) x).collect(Collectors.toSet());
        System.out.println(inds);
    }
}
