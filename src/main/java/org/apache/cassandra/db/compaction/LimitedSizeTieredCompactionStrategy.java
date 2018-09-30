/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class LimitedSizeTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(LimitedSizeTieredCompactionStrategy.class);

    private static final Comparator<Pair<List<VirtualSSTable>,Double>> bucketsByHotnessComparator = new Comparator<Pair<List<VirtualSSTable>, Double>>()
    {
        public int compare(Pair<List<VirtualSSTable>, Double> o1, Pair<List<VirtualSSTable>, Double> o2)
        {
            int comparison = Double.compare(o1.right, o2.right);
            if (comparison != 0)
                return comparison;

            // break ties by compacting the smallest sstables first (this will probably only happen for
            // system tables and new/unread sstables)
            return Long.compare(avgSize(o1.left), avgSize(o2.left));
        }

        private long avgSize(List<VirtualSSTable> vsstables)
        {
            long n = 0;
            for (VirtualSSTable vsstable : vsstables)
                n += vsstable.bytesOnDisk;
            return n / vsstables.size();
        }
    };

    protected LimitedSizeTieredCompactionStrategyOptions limitedSizeTieredOptions;
    protected volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();

    public LimitedSizeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.limitedSizeTieredOptions = new LimitedSizeTieredCompactionStrategyOptions(options);
    }

    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        // make local copies so they can't be changed out from under us mid-method
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));

        List<List<VirtualSSTable>> buckets = getBuckets(createSSTableAndLengthPairs(candidates), limitedSizeTieredOptions.bucketHigh, limitedSizeTieredOptions.bucketLow, limitedSizeTieredOptions.minSSTableSize);
        logger.trace("Compaction buckets are {}", buckets);
        updateEstimatedCompactionsByTasks(buckets);
        List<SSTableReader> mostInteresting = mostInterestingBucket(buckets, minThreshold, maxThreshold);
        if (!mostInteresting.isEmpty())
            return mostInteresting;

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        Collections.sort(sstablesWithTombstones, new SSTableReader.SizeComparator());
        return Collections.singletonList(sstablesWithTombstones.get(0));
    }


    /**
     * @param buckets list of buckets from which to return the most interesting, where "interesting" is the total hotness for reads
     * @param minThreshold minimum number of sstables in a bucket to qualify as interesting
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this)
     * @return a bucket (list) of sstables to compact
     */
    public static List<SSTableReader> mostInterestingBucket(List<List<VirtualSSTable>> buckets, int minThreshold, int maxThreshold)
    {
        // skip buckets containing less than minThreshold sstables, and limit other buckets to maxThreshold sstables
        final List<Pair<List<VirtualSSTable>, Double>> prunedBucketsAndHotness = new ArrayList<>(buckets.size());
        for (List<VirtualSSTable> bucket : buckets)
        {
            Pair<List<VirtualSSTable>, Double> bucketAndHotness = trimToThresholdWithHotness(bucket, maxThreshold);
            if (bucketAndHotness != null && bucketAndHotness.left.size() >= minThreshold)
                prunedBucketsAndHotness.add(bucketAndHotness);
        }
        if (prunedBucketsAndHotness.isEmpty())
            return Collections.emptyList();

        Pair<List<VirtualSSTable>, Double> hottest = Collections.max(prunedBucketsAndHotness, bucketsByHotnessComparator);
        return hottest.left.stream().map(vsstable -> vsstable.sstables).flatMap(List::stream).collect(Collectors.toList());
    }

    /**
     * Returns a (bucket, hotness) pair or null if there were not enough sstables in the bucket to meet minThreshold.
     * If there are more than maxThreshold sstables, the coldest sstables will be trimmed to meet the threshold.
     **/
    @VisibleForTesting
    static Pair<List<VirtualSSTable>, Double> trimToThresholdWithHotness(List<VirtualSSTable> bucket, int maxThreshold)
    {
        // Sort by sstable hotness (descending). We first build a map because the hotness may change during the sort.
        final Map<VirtualSSTable, Double> hotnessSnapshot = getHotnessMap(bucket);
        Collections.sort(bucket, new Comparator<VirtualSSTable>()
        {
            public int compare(VirtualSSTable o1, VirtualSSTable o2)
            {
                return -1 * Double.compare(hotnessSnapshot.get(o1), hotnessSnapshot.get(o2));
            }
        });

        // and then trim the coldest sstables off the end to meet the maxThreshold
        List<VirtualSSTable> prunedBucket = bucket.subList(0, Math.min(bucket.size(), maxThreshold));

        // bucket hotness is the sum of the hotness of all sstable members
        double bucketHotness = 0.0;
        for (VirtualSSTable sstr : prunedBucket)
            bucketHotness += hotness(sstr);

        return Pair.create(prunedBucket, bucketHotness);
    }

    private static Map<VirtualSSTable, Double> getHotnessMap(Collection<VirtualSSTable> vsstables)
    {
        Map<VirtualSSTable, Double> hotness = new HashMap<>();
        for (VirtualSSTable vsstable : vsstables)
        {
            hotness.put(vsstable, hotness(vsstable));
        }
        return hotness;
    }

    /**
     * Returns the reads per second per key for this sstable, or 0.0 if the sstable has no read meter
     */
    private static double hotness(SSTableReader sstr)
    {
        // system tables don't have read meters, just use 0.0 for the hotness
        return sstr.getReadMeter() == null ? 0.0 : sstr.getReadMeter().twoHourRate() / sstr.estimatedKeys();
    }

    private static double hotness(VirtualSSTable vsstr)
    {
        double vhotness = 0.0;
        for (SSTableReader sstable : vsstr.sstables)
            vhotness += hotness(sstable);
        return vhotness;
    }

    private static int getLevel(Iterable<SSTableReader> sstables)
    {
        int maxGen = 0;
        for (SSTableReader sstable : sstables)
            maxGen = Math.max(maxGen, sstable.descriptor.generation);
        return maxGen;
    }

    @SuppressWarnings("resource")
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        while (true)
        {
            List<SSTableReader> hottestBucket = getNextBackgroundSSTables(gcBefore);

            if (hottestBucket.isEmpty())
                return null;

            LifecycleTransaction transaction = cfs.getTracker().tryModify(hottestBucket, OperationType.COMPACTION);
            if (transaction != null)
                return new LeveledCompactionTask(cfs, transaction, getLevel(hottestBucket),
                                                 gcBefore, limitedSizeTieredOptions.targetSSTableSize, false);
        }
    }

    @SuppressWarnings("resource")
    public Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;

        return Arrays.asList(new LeveledCompactionTask(cfs, txn, getLevel(filteredSSTables),
                                                       gcBefore, limitedSizeTieredOptions.targetSSTableSize, false));
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new LeveledCompactionTask(cfs, transaction, getLevel(sstables),
                                         gcBefore, limitedSizeTieredOptions.targetSSTableSize, false).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public static List<Pair<VirtualSSTable, Long>> createSSTableAndLengthPairs(Iterable<SSTableReader> sstables)
    {
        List<Pair<VirtualSSTable, Long>> sstableLengthPairs = new ArrayList<>(Iterables.size(sstables));
        Map<Integer, List<SSTableReader>> sstablesByLevel = new HashMap<>();

        for (SSTableReader sstable : sstables)
        {
            int level = sstable.getSSTableLevel();
            if (level == 0)
                level = sstable.descriptor.generation;

            if (!sstablesByLevel.containsKey(level))
                sstablesByLevel.put(level, new ArrayList<>());

            sstablesByLevel.get(level).add(sstable);
        }

        for (Integer level : sstablesByLevel.keySet())
        {
            VirtualSSTable vsstable = new VirtualSSTable(sstablesByLevel.get(level));
            sstableLengthPairs.add(Pair.create(vsstable, vsstable.size));
        }
        return sstableLengthPairs;
    }

    /*
     * Group files of similar size into buckets.
     */
    public static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, double bucketHigh, double bucketLow, long minSSTableSize)
    {
        // Sort the list in order to get deterministic results during the grouping below
        List<Pair<T, Long>> sortedFiles = new ArrayList<>(files);
        sortedFiles.sort(Comparator.comparing(p -> p.right));

        Map<Long, List<T>> buckets = new HashMap<>();

        outer:
        for (Pair<T, Long> pair: sortedFiles)
        {
            long size = pair.right;

            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
            for (Entry<Long, List<T>> entry : buckets.entrySet())
            {
                List<T> bucket = entry.getValue();
                long oldAverageSize = entry.getKey();
                if ((size > (oldAverageSize * bucketLow) && size < (oldAverageSize * bucketHigh))
                    || (size < minSSTableSize && oldAverageSize < minSSTableSize))
                {
                    // remove and re-add under new new average size
                    buckets.remove(oldAverageSize);
                    long totalSize = bucket.size() * oldAverageSize;
                    long newAverageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(pair.left);
                    buckets.put(newAverageSize, bucket);
                    continue outer;
                }
            }

            // no similar bucket found; put it in a new one
            ArrayList<T> bucket = new ArrayList<T>();
            bucket.add(pair.left);
            buckets.put(size, bucket);
        }

        return new ArrayList<>(buckets.values());
    }

    private void updateEstimatedCompactionsByTasks(List<List<VirtualSSTable>> tasks)
    {
        int n = 0;
        for (List<VirtualSSTable> bucket: tasks)
        {
            if (bucket.size() >= cfs.getMinimumCompactionThreshold())
                n += Math.ceil((double)bucket.size() / cfs.getMaximumCompactionThreshold());
        }
        estimatedRemainingTasks = n;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = LimitedSizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(LimitedSizeTieredCompactionStrategyOptions.MIN_THRESHOLD);
        uncheckedOptions.remove(LimitedSizeTieredCompactionStrategyOptions.MAX_THRESHOLD);

        return uncheckedOptions;
    }

    @Override
    public boolean shouldDefragment()
    {
        return true;
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    public String toString()
    {
        return String.format("LimitedSizeTieredCompactionStrategy[%s/%s:%s mb]",
                             cfs.getMinimumCompactionThreshold(),
                             cfs.getMaximumCompactionThreshold(), limitedSizeTieredOptions.targetSSTableSize / (1024 * 1024));
    }
}
