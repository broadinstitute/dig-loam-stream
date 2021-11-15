# LoamStream
LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.

## Building
### Requirements
  - Git
  - Java 8
  - SBT 1.5.0+
### To build a runnable LoamStream binary from the master branch:
  - Clone the repository at https://github.com/broadinstitute/dig-loam-stream
  - Build a fat jar with `sbt assembly`
### To run the unit tests:
  - `sbt test`
### To run the integration tests:
  - `sbt it:test`
  - This is best done from Jenkins, or at a minimum, from a Broad VM that can submit Uger jobs.
    - See [JENKINS.md]

## Running
- See [CLI.md] and [LOAM.md]
