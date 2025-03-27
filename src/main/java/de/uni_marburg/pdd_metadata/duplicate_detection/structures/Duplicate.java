package de.uni_marburg.pdd_metadata.duplicate_detection.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;

@Getter
@AllArgsConstructor
public class Duplicate implements Comparable<Duplicate> {
    private int index1;
    private int index2;
    private String recordId1;
    private String recordId2;
    private long timestamp;

    public Duplicate(String recordId1, String recordId2, long timestamp) {
        this.index1 = -1;
        this.index2 = -1;
        this.recordId1 = recordId1;
        this.recordId2 = recordId2;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Duplicate duplicate = (Duplicate) o;

        return (Objects.equals(recordId1, duplicate.recordId1) && Objects.equals(recordId2, duplicate.recordId2)) ||
                (Objects.equals(recordId1, duplicate.recordId2) && Objects.equals(recordId2, duplicate.recordId1));
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordId1.compareTo(recordId2) <= 0 ? recordId1 : recordId2, recordId1.compareTo(recordId2) > 0 ? recordId1 : recordId2);
    }

    @Override
    public int compareTo(Duplicate otherDuplicate) {
        if (this.timestamp < otherDuplicate.getTimestamp()) {
            return -1;
        } else {
            return this.timestamp > otherDuplicate.getTimestamp() ? 1 : 0;
        }
    }
}
