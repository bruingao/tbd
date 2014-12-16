ThomasDB
===========

Build with SBT (you don't need to install SBT on your machine)

```
> sbt/sbt
> compile
> test
```

## Experiment
```
> sbt/sbt
> mkrun
```

This will create the script bin/experiment.sh which can be used to run the
performance experiments.

To run in cluster mode, 'mkrun' will also create the script bin/master.sh
which can be used to launch a master. This script will print out a URL of
the form 'akka.tcp://...' which can then be passed to the bin/worker.sh
script as a required trailing argument. Then, you can run bin/experiment.sh with
the '--master' flag with this URL.

## Debugging

To turn on debugging output from the library, you can change the imports at 
the top of any source files containing self-adjusting code from:

```
import tbd._
import tbd.TBD._
```

to

```
import tbd.debug._
import tbd.debug.TBD._
```

This is required to make the visualizer work, for example modifying ModList.scala and
ModListNode.scala.

## Visualizing

The visualizer can be used to visualize traces and dependencies. Furthermore, the visualizer can be used to calculate intrinsic trace distance. Make sure to turn on debugging, as described above, to use the visualizer.

```
> sbt/sbt
> mkvisualization
```

Run examples:

```
> bin/visualization.sh --help
> bin/visualization.sh -a sort
> bin/visualization.sh -a map -o diff
> bin/visualization.sh -a map -o diff -t manual
> bin/visualization.sh -a sort -o plot2d -t exhaustive
> bin/visualization.sh -a reduce -i 5 -o chart2d -t exhaustive
```

To call a quick visualizer from within your code (for debugging), use: 

```
tbd.visualization.QuickVisualizer.show(mutator.getDDG())
```

*Visualizer UI*

Select mutation round from combo box.
Click node for details in lower panel
Black edges: Control dependency.
Red edges: Mod dependencies (mod <-> write or read <-> write)
Dashed edges: Bound free vars, whereas color corresponds to color of description

If two different DDGs are selected in diff mode, the program will show intrinsic trace distance and distance accomplished by TBD change propagation in the lowermost panel.
Removed/Added nodes will be highlighted in yellow/red.

Scroll by dragging the mouse.
Zoom with the scroll wheel. 

## REEF
```
> sbt/sbt
> assembly
```

This will create the assembly jar file reef/target/scala-2.11/reef-assembly-0.1-SNAPSHOT.jar which can be submitted to REEF to run distributed TBD on HADOOP V2 YARN.

Run cluster mode with REEF on YARN:
```
$HADOOP_HOME/bin/yarn jar reef/target/scala-2.11/reef-assembly-0.1-SNAPSHOT.jar tbd.reef.TBDLaunch -local false -numWorkers 2 -timeout 10 -memory 8192 -workerXss 4
```

Wait until master akka system ("akka.tcp://masterSystem0@127.0.53.53:2555/user/master" in the following example) is return to client, then submit an experiment to the cluster, and add -c parameter to check correctness of the output:
```
bin/experiment.sh -a qsort --chunkSizes 1 --partitions 2 -n 10 -c --master akka.tcp://masterSystem0@127.0.53.53:2555/user/master
```

The client is interactive: type in "master" will return master akka; type in "workers" will return workers' ip and port.


Run cluster mode with REEF data loading on YARN:
```
$HADOOP_HOME/bin/yarn jar reef/target/scala-2.11/reef-assembly-0.1-SNAPSHOT.jar tbd.reef.dataloading.DataLoadingReefYarn -local false -timeout 10 -input file:///wiki.xml -partitions 2 -chunkSizes 1
```

Wait until master akka system address is return to client, then submit an experiment to the cluster without generating data.
