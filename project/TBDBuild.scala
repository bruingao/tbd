import sbt._
import Keys._

object TBDBuild extends Build {
  val buildOrganization = "edu.cmu.cs"
  val buildVersion      = "0.1-SNAPSHOT"
  val buildScalaVersion = "2.11.0"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion,
    fork         := true,
    autoScalaLibrary := true,
    scalacOptions ++= Seq("-feature", "-deprecation")
  )

  val mavenResolver = "Maven Central Server" at "http://central.maven.org/maven2"

  val commonDeps = Seq (
    "berkeleydb"                  % "je"                   % "3.2.76",

    "com.typesafe.akka"          %% "akka-actor"           % "2.3.2",
    "com.typesafe.scala-logging" %% "scala-logging-slf4j"  % "2.0.4",

    "org.scala-lang"             %% "scala-pickling"       % "0.8.0",
    "org.scalatest"              %% "scalatest"            % "2.1.3" % "test",
    "org.scalaz"                 %% "scalaz-core"          % "7.0.6"
  )

  val mkrun = TaskKey[File]("mkrun")
  val mkexamples = TaskKey[File]("mkexamples")
  val mkvisualization = TaskKey[File]("mkvisualization")

  lazy val root = Project (
    "root",
    file(".")
  ) aggregate(core, examples, visualization)

  lazy val core = Project (
    "core",
    file("core"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= commonDeps,
      resolvers += mavenResolver,
      javaOptions += "-Xss128M",
      mkrun := {
        val classpath = (fullClasspath in Runtime).value.files.absString
        val template = """#!/bin/sh
        java -Xmx2g -Xss4m -classpath "%s" %s $@
        """

        val master = template.format(classpath, "tbd.master.Main")
        val masterOut = baseDirectory.value / "../bin/master.sh"
        IO.write(masterOut, master)
        masterOut.setExecutable(true)

        masterOut
      }
    )
  )

  lazy val visualization = Project(
    "visualization",
    file("visualization"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= (commonDeps
                          ++ Seq("org.graphstream" % "gs-core" % "1.2",
                                 "org.graphstream" % "gs-ui" % "1.2",
                                 "org.scala-lang" % "scala-swing" % "2.11.0-M7")),
      mkvisualization := {
        val classpath = (fullClasspath in Runtime).value.files.absString
        val template = """#!/bin/sh
        java -Xmx8g -Xss256m -classpath "%s" %s $@
        """

        val visualization = template.format(classpath, "tbd.visualization.Main")
        val visualizationOut = baseDirectory.value / "../bin/visualization.sh"
        IO.write(visualizationOut, visualization)
        visualizationOut.setExecutable(true)

        visualizationOut
      }
    )
  ) dependsOn(core)

  lazy val examples = Project(
    "examples",
    file("examples"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= commonDeps,
      mkexamples := {
        val classpath = (fullClasspath in Runtime).value.files.absString
        val template = """#!/bin/sh
        java -Xmx8g -Xss256m -classpath "%s" %s $@
        """

        val experiment = template.format(classpath, "tbd.examples.list.Experiment")
        val experimentOut = baseDirectory.value / "../bin/experiment.sh"
        IO.write(experimentOut, experiment)
        experimentOut.setExecutable(true)

        val test = template.format(classpath, "tbd.examples.Test")
        val testOut = baseDirectory.value / "../bin/test.sh"
        IO.write(testOut, test)
        testOut.setExecutable(true)

        experimentOut
      }
    )
  ) dependsOn(core, visualization)
}
