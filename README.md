DataNode Volumes Rebalancing tool for Apache Hadoop HDFS
===============

This project aims at filling the gap with [HDFS-1312](https://issues.apache.org/jira/browse/HDFS-1312) & family: when a hard drive dies on a Datanode and gets replaced, there is not real way to move blocks from most used hard disks to the newly added -- and thus empty.

[HDFS-1804](https://issues.apache.org/jira/browse/HDFS-1804) is a really good addition that allows the DataNode to choose the least used hard drive, but for new blocks only. Balancing existing blocks is still something missing and thus the main focus of this project.

# Usage
<pre>
Available options:
-help , this information -threshold=d, default 0.0001, try to restrict within the threshold
-concurrency=n, default 10, min(10,volumeNum/2)
-submit, trust VB without interative
-unbalance, unbalance the volume
-balance, balance the volume
-rollback=vb_undoxxxx.log, get moved back by one thread, useless at most time except for test
P.S.: adding both -unbalance then -balance means first unbalance and then balance it, mostly in simulateMode(not adding -submit)
org.apache.hadoop.hdfs.server.datanode.VolumeBalancer``
</pre>

# Parameters

## -threshold

Default value: 0.0001 (0.01%)
Required Range: [0.0001,0.1]
The script stops when all the drives reach the average disks utilization +/- the threshold.
The smaller the value, the more balanced the drives will be and the longer the process will take.
Because subdir is the smallest unit we can operate in the volume-balancer, it
hardly lets all volumes are in the precisely equal size.
E.g. Volumes will balanced in a range [0.8-0.001,0.8+0.001]

## -concurrency

Default value: 1 (no concurrency)
Automaticlly striction: min(10,volumeNum/2)

Define how many threads are concurrently reading (from different disks) and writing to the least used disk(s).
Advised value is min(10,volumeNum/2), increasing the concurrency does not *always* increase the overall throughput.
Because we promise that per volume can be used in only one subdir move,
        fromVolume or toVolume.

## -balance        
You can use like the following command, then the threshold and concurrency
option will be the defaule value above.
<pre>
hadoop jar volume-balancer-1.2-SNAPSHOT-jar-with-dependencies.jar  -balance
</pre>

## -unbalance
Just like the -balance, this option do the opposite effect. it will let the
volumes unbalanced. Most time, this option is used for testing.

## -submit 
For data is so important in the datanode, we cannot just move it without
confirming the moving strategy. no "-submit" option, the above "-balance" and "-unbalance" will be just the simulation mode without real data move.
<pre>
hadoop jar volume-balancer-1.2-SNAPSHOT-jar-with-dependencies.jar  -balance
-submit
</pre>

## -rollback
When do subdir move, volume balancer will write down the all the moving records,
     such as from /volume_a/subdir0 ---> /volume_b/subdir1
<pre>
hadoop jar volume-balancer-1.2-SNAPSHOT-jar-with-dependencies.jar
-rollback=vb_undo_2015-03-26_18_25_01.log -concurrency=10
</pre>

In fact, the rollback command mostly is not usable, just for testing and
debuging, we have already promised the
data completion and verivation by checksum and length compation.
if some file is broken, it mostly caused not by volume-balancer but by other
reasons out of volume-balancer.


# How it works

The script will classify all the volumes into 4 groups:
1. farBelowAvarageUsable
2. thresholdAvarageUsable
3. farAboveAvarageUsable
4. thresholdAvarageUsable

then take a suitable ``subdir*`` from the farBelow to farAbove, for those volume
already within the threshold, we will never touch it.
for the selected "from Subdir", we will move it to a target ``subdir*`` (not exceeding ``DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY``) of the least used partition.
The ``subdir`` keyword comes from ``DataStorage.BLOCK_SUBDIR_PREFIX``

The script is doing pretty good job at keeping the bandwidth of the least used hard drive maxed out using
``FileUtils#moveDirectory(File, File)`` and a dedicated ``j.u.c.ExecutorService`` for the copy. Increasing the
concurrency of the thread performing the copy does not *always* help to improve disk utilization, more particularly
at the target disk. But if you use -concurrency > 1, the script is balancing the read (if possible) amongst several
disks.
Whats' more, for monitoring the copy progress, we implement the FileUtils with
reporter and file checksum.  

Once all disks reach the disks average utilization +/- threshold (can be given as input parameter, by default 0.1)
the script stops. But it can also be **safely stopped** at any time hitting **Crtl+C**: it shuts down properly ensuring **ALL
blocks** of a ``subdir`` are moved, leaving the datadirs in a proper state.

# Monitoring

Appart of the standard ``df -h`` command to monitor disks fulling in, the disk bandwidths can be easily monitored using ``iostat -x 1 -m``

```
$ iostat -x 1 -m
   Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await  svctm  %util
   sdd               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
   sde               0.00 32911.00    0.00  300.00     0.00   149.56  1020.99   138.72  469.81   3.34 100.00
   sdf               0.00    27.00  963.00   50.00   120.54     0.30   244.30     1.37    1.35   0.80  80.60
   sdg               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
   sdh               0.00     0.00  610.00    0.00    76.25     0.00   255.99     1.45    2.37   1.44  88.10
   sdi               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
```

## License

Copyright © 2013-2014 Jie Cao

See [LICENSE](LICENSE) for licensing information.

# Contributions

All contributions are welcome: ideas, documentation, code, patches, bug reports, feature requests etc.  And you don't
need to be a programmer to speak up!

If you are new to GitHub please read [Contributing to a project](https://help.github.com/articles/fork-a-repo) for how
to send patches and pull requests to this project.

