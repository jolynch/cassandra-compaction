package org.apache.cassandra.db.compaction;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import java.util.List;

/**
 * A "Virtual" sstable that contains other actually on disk sstables. This can be used to treat groups of
 * sstables as a single logical sstable for the purposes of e.g. compaction.
 */
class VirtualSSTable {
    public final ImmutableList<SSTableReader> sstables;
    public final Long size;
    public final Long bytesOnDisk;

    public VirtualSSTable(List<SSTableReader> sstables)
    {
        this.sstables = ImmutableList.copyOf(sstables);
        this.size = sstables.stream().mapToLong(SSTableReader::onDiskLength).sum();
        this.bytesOnDisk = sstables.stream().mapToLong(SSTableReader::bytesOnDisk).sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VirtualSSTable that = (VirtualSSTable) o;

        if (!sstables.equals(that.sstables)) return false;
        if (!size.equals(that.size)) return false;
        return bytesOnDisk.equals(that.bytesOnDisk);
    }

    @Override
    public int hashCode() {
        int result = sstables.hashCode();
        result = 31 * result + size.hashCode();
        result = 31 * result + bytesOnDisk.hashCode();
        return result;
    }
}
