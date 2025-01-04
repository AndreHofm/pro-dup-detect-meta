package de.pdd_metadata.duplicate_detection.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Duplicate implements Comparable<Duplicate> {
    private int recordId1;
    private int recordId2;


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
