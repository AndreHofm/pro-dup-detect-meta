package de.pdd_metadata.duplicate_detection.structures.progressive_blocking;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class BlockResult implements Comparable<BlockResult> {
    private int firstBlockId;
    private int secondBlockId;
    private int duplicates;
    private int keyId;

    @Override
    public int compareTo(BlockResult other) {
        if (this.duplicates == other.getDuplicates()) {
            int distanceDifference = this.secondBlockId - this.firstBlockId - (other.getSecondBlockId() - other.getFirstBlockId());
            if (distanceDifference == 0) {
                return 0;
            } else {
                return distanceDifference < 0 ? -1 : 1;
            }
        } else {
            return this.duplicates > other.getDuplicates() ? -1 : 1;
        }
    }
}
