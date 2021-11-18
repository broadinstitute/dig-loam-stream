# Loam Pipeline Description Language

## Overview

Loam is a high-level language for declaratively encoding computational pipelines.  It tracks and infers dependencies between jobs, allowing jobs to be specified out-of-order.  Loam code can be split beween multiple files, and since it's a dialect of Scala, it can use any constructs provided by Scala's standard library.  (And more.)

## Execution Model

Loam code is evaluated from top-to-bottom, but relationships between components of a pipeline are not inferred until after evaluation is complete.  In general, this means jobs can be specified in any order, provided their inputs and outputs are described correctly.  

It's important to note that evaluating Loam code, which could involve executing arbitrary code, even from external libraries, loading data from config files, invoking remote APIs, etc, happens _before_ any jobs described by the Loam code are run.

This document will use _evaluation-time_ to refer to the phase when Loam code is evaluated, and _execution-time_ to refer to the phase where the pipeline encoded in the Loam files is actually run, but executing its component jobs in the order in which they depend on each other.

## Backends

Loam is evaluated by LoamStream, the application, to produce a graph describing the relationships between jobs in a pipeline.  When the pipeline is executed, the graph is effectively traversed in topological order, and jobs are dispatched to various backends for execution.  Supported backends are:

- Local: the local machine.  This isn't used very often, but is used implicitly by LoamStream for some small tasks, and is useful for testing small pipelines.
- DRM systems: clusters, HPC, etc 
  - Uger
  - Slurm
  - LSF 
- Google: theoretically for anything that can run on a GCP Dataproc cluster, but only used for Hail support.

Backends can be mixed and matched heterogeneously in a pipeline. For example, a command running locally can produce output that's used by a Uger job, which is in turn run by a Hail job at Google, and so on.

## Concepts

The two principal building blocks in loam code are stores and jobs.  Stores correspond to data that's produced or consumed by jobs.  Specify them like this:

```scala
val someFile = store("foo.txt")

val someFileAtGCS = store(uri("gs://some-bucket/some/file"))

// A store backed by a file in a temporary directory.  This directory is configurable via the
// loamstream.executionConfig.anonStoreDir knob in loamstream.conf
val anonymous = store()
```

Jobs are things you want to run.  Most commonly, they're commands, but they could be Hail invocations, or even blocks of arbitrary Loam code:

```scala
val someFile = store("foo.txt")

cmd"echo 42 > $someFile"  //The cmd interpolator makes command-lines that can be run locally or on a DRM system
  .out(someFile)          //specifying inputs and outputs is mandatory, except when there are none.  Here there are 
                          //no inputs.
  .tag("my-echo-command") //A mandatory human-readable name for the job.  Can be the result of arbitrary code.
```

## Hello World

Here's a complete self-contained example.

In Foo.scala:
```scala
//All Loam code must be within an object that extends loamstream.LoamFile, and is named the same as the file it's
//defined in, in the default package (ie, omit a package declaration at the top of the file).
object Foo extends loamstream.LoamFile {
  cmd"echo Hello World".tag("my-echo-command")
}
```

Running this with `java -jar path/to/loamstream.jar --loams Foo.scala` produces the following artifacts in `.loamstream/jobs`:
```
user@host:~/workspace/dig-loam-stream/.loamstream/jobs$ find
...
./data/my-echo-command/stdout
./data/my-echo-command/accounting-summary
./data/my-echo-command/settings

user@host:~/workspace/dig-loam-stream/.loamstream/jobs$ cat ./data/my-echo-command/stdout
Hello World
```

Here's a more-complex one:
```scala
object Foo extends loamstream.LoamFile {

  val helloWorld = store("hello-world.txt")

  cmd"echo Hello World > $helloWorld".out(helloWorld).tag("my-echo-command")

  val lineCount = store("count")

  cmd"wc -l $helloWorld > $count".in(helloWorld).out(lineCount).tag("count-lines")

  //A loam builtin, parses the specified file as HOCON or JSON using typesafe-config
  //(see https://github.com/lightbend/config) returning a DataConfig object; see
  //https://github.com/broadinstitute/dig-loam-stream/blob/master/src/main/scala/loamstream/conf/DataConfig.scala
  val config = loadConfig(path("foo.conf"))

  for {
    i <- 1 to config.getInt("numBars")
  } {
    val out = store(s"out/copy-$i".txt)

    cmd"cp $lineCount $out".in(lineCount).out(out).tag(s"copy-$i")
  }
}
```