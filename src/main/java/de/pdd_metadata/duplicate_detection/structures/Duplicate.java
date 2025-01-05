package de.pdd_metadata.duplicate_detection.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Duplicate implements Comparable<Duplicate> {
    private int recordId1;
    private int recordId2;
    private int posRecordId1;
    private int posRecordId2;

    public Duplicate(int recordId1, int recordId2) {
        this.recordId1 = recordId1;
        this.recordId2 = recordId2;
    }


    @Override
    public int compareTo(Duplicate o) {
        if (this.recordId1 < o.getRecordId1())
            return -1;
        if (this.recordId1 > o.getRecordId1())
            return 1;
        if (this.recordId2 < o.getRecordId2())
            return -1;
        if (this.recordId2 > o.getRecordId2())
            return 1;
        return 0;
    }
}
