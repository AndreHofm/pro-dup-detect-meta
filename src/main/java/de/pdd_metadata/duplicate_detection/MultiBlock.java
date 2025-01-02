package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.multi_block.BlockingStructure;
import de.pdd_metadata.duplicate_detection.structures.multi_block.NCVR_Record;
import de.pdd_metadata.duplicate_detection.structures.multi_block.RecordPair;

import java.util.HashSet;
import java.util.Set;

public class MultiBlock {

    public static void execute(BlockingStructure H) {
        // BlockingStructure H = new BlockingStructure();
        double ratio = 33.0;
        double delta = 0.1;
        int s = H.calcS(delta, ratio);
        H.init(); // init Priority Queue
        HashSet<RecordPair> matchingPairs = new HashSet<>();

        for (int i = 1; i <= s; i++) {
            // Arrays.sort(H.pq.toArray());
            for (Block block : H.pq) {
                Set<RecordPair> set = block.sample(ratio);
                for (RecordPair rp : set) {
                    NCVR_Record rA = (NCVR_Record) rp.rA;
                    NCVR_Record rB = (NCVR_Record) rp.rA;
                    block.evaluations++;
                    if (NCVR_Record.eval(rA, rB)) {
                        matchingPairs.add(rp);
                        block.noMatchingPairs++;
                    }
                    block.calcScore();
                    H.pq.add(block);
                }
            }
        }
    }
}
