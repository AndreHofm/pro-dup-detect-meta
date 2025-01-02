package de.pdd_metadata.duplicate_detection.structures.progressive_blocking;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class BlockResult {
    private int firstBlockId;
    private int secondBlockId;
    private int duplicates;
    private int keyId;
}
