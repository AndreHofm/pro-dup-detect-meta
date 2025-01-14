package de.pdd_metadata.data_profiling;

import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Sandy;
import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.util.IND;
import de.hpi.isg.sindy.util.PartialIND;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import lombok.Getter;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Getter
public class INDProfiler {
    private Sindy sindy;
    private Sandy sandy;
    private Set<IND> inds = new HashSet<>();
    private Set<PartialIND> partialINDS = new HashSet<>();

    public INDProfiler(String path1, String path2) {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(path1, path2),
                numColumnBits
        );

        ExecutionEnvironment ex = ExecutionEnvironment.getExecutionEnvironment();
        ex.setParallelism(1);
        this.sindy = new Sindy(indexedInputFiles, numColumnBits, ex, inds::add);
        this.sandy = new Sandy(indexedInputFiles, numColumnBits, ex, partialINDS::add);
    }

    public void executePartialINDProfiler() {
        sandy.setDropNulls(false);
        sandy.setFieldSeparator('\t');
        sandy.setMaxArity(1);
        sandy.setMinRelativeOverlap(0.0);
        sandy.run();
    }

    public void executeFullINDProfiler() {
        sindy.setDropNulls(true);
        sindy.setNullString("\\N");
        sindy.setFieldSeparator('\t');
        sindy.setMaxArity(1);
        sindy.run();
    }
}
