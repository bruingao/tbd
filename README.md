TBD
===========

Build with SBT (you don't need to install SBT on your machine)

```
> sbt/sbt
> compile
> test
```

## Experiment
```
>sbt/sbt
> mkexamples
```

This will create the script bin/experiment.sh which can be used to run the
performance experiments.

## Debuggins

To turn on debug log output, edit core/src/main/resources/common.conf, changing
loglevel to "DEBUG".

To turn on more useful DDG output, edit core/src/main/scala/tbd/master/Main.scala,
changing 'val debug' to 'true'. Note: this will cause tests using MockDDG to
fail. Just ignore that.