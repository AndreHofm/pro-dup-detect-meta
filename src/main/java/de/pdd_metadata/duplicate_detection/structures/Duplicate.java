package de.pdd_metadata.duplicate_detection.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Objects;

@Getter
@AllArgsConstructor
public class Duplicate implements Comparable<Duplicate> {
    private int index1;
    private int index2;
    private int recordId1;
    private int recordId2;

    public Duplicate(int recordId1, int recordId2) {
        this.index1 = -1;
        this.index2 = -1;
        this.recordId1 = Math.min(recordId1, recordId2);
        this.recordId2 = Math.max(recordId1, recordId2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Duplicate duplicate = (Duplicate) o;

        return (recordId1 == duplicate.recordId1 && recordId2 == duplicate.recordId2) ||
                (recordId1 == duplicate.recordId2 && recordId2 == duplicate.recordId1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Math.min(recordId1, recordId2), Math.max(recordId1, recordId2));
    }

    @Override
    public int compareTo(Duplicate o) {
        int[] thisValues = {this.index1, this.index2, this.recordId1, this.recordId2};
        int[] otherValues = {o.index1, o.index2, o.recordId1, o.recordId2};
        Arrays.sort(thisValues);
        Arrays.sort(otherValues);

        for (int i = 0; i < 4; i++) {
            int comp = Integer.compare(thisValues[i], otherValues[i]);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }
}
