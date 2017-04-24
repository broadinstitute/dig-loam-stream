# LoamStream
LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.

## Jenkins:
  - `$CI_ROOT`: `/humgen/diabetes/users/dig/loamstream/ci/`
  - `$JENKINS_ROOT`: `${CI_ROOT}/jenkins/`
  - War: `${JENKINS_ROOT}/jenkins-2.32.1-LTS.war`
  - `~diguser/.jenkins` symlinked to `${JENKINS_ROOT}/home`, so Jenkins looks in its default place for files.
  - Workspace (builds are cloned to and run from here): `${JENKINS_ROOT}/home/workspace/`
  - Running Jenkins:
    - Use `${JENKINS_ROOT}/bin/run-jenkins.sh` (`use`s various tools, sets Google-required env vars, makes running LS integration tests possible.)
    - Jenkins runs in the `jenkins` screen session for the user `diguser` on `dig-ae-dev-01`.

## Integration tests:
- Live in `src/it/scala/`
- Run with `it:test`, either from the SBT prompt or when starting SBT (`sbt it:test`)

`loamstream.QcPipelineEndToEndTest`:
 - Runs `qc.loam` through LS, equivalent to running 
  ```
  scala -jar loamstream.jar --conf pipeline/loam/qc.conf pipeline/loam/binaries.loam pipeline/loam/cloud_helpers.loam \
                                   pipeline/loam/input.loam pipeline/loam/qc.loam pipeline/loam/scripts.loam \
                                   pipeline/loam/store_helpers.loam
  ```
  from the command line.
  - Checks some outputs (see `QcPipelineEndToEndTest.scala` for a list) against assumed-good ones from a manual run stored in `/humgen/diabetes/users/dig/loamstream/ci/test-data/qc/camp/results/`.

## Detailed Jenkins Setup Log:

### Dirs made:
  - `/humgen/diabetes/users/dig/loamstream/ci/jenkins/wars/`
    contains `jenkins-2.32.1-LTS.war`
  - `/humgen/diabetes/users/dig/loamstream/ci/jenkins/home/`
    symlinked from `~diguser/.jenkins`
### First-time startup
- Started Jenkins with 
  `java -jar /humgen/diabetes/users/dig/loamstream/ci/jenkins/wars/jenkins-2.32.1-LTS.war`
- Gave initial, generated admin password from
  `~diguser/.jenkins/secrets/initialAdminPassword`
- Installed suggested plugins
- Installed SBT plugin:
  - Dashboard => 'Manage Jenkins' => 'Manage Plugins' => 'Available' => search for 'sbt'
  - Install this one: http://wiki.jenkins-ci.org/display/JENKINS/sbt+plugin
- Restart Jenkins
### Point to JDK and SBT
- Dashboard => Manage Jenkins => Global Tool Configuration
  - JDK:
    - uncheck 'install automatically'
    - Name: 'JDK8' (purely descriptive)
    - `JAVA_HOME`: `/broad/software/free/Linux/redhat_6_x86_64/pkgs/jdk1.8.0_121`
    - TODO: get Oracle account, enable 'install automatically', don't hard-code dotkit-supplied `JAVA_HOME`
  - SBT:
    - install automatically
      - install from scala-sbt.org
        - version: `sbt 0.13.13`
    - `name: `SBT 0.13.13`
    - sbt launch arguments: `-XX:ReservedCodeCacheSize=256m -XX:MaxMetaspaceSize=512m -Xms1G -Xmx2G -XX:+AggressiveOpts -XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC`
### Creating a first job
- Dashboard => 'Create New Job'
  - name: LoamStream
  - type: 'Freestyle project'
  - General tab:
    - check 'github project', url: `https://github.com/broadinstitute/dig-loam-stream`
    - Source Code Management => Git
      - repository url: `https://github.com/broadinstitute/dig-loam-stream.git`
    - credentials => add => jenkins
      - used `ClintAtTheBroad` for now (TODO)
    - branches to build: just master for now
  - Build Triggers:
    - check 'poll SCM'
      - schedule: `H/15 * * * *` (every 15 minutes; could be more often)
  - Build:
    - Add build step => 'build using sbt'
      - actions: `clean test`
