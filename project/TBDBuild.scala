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
    scalacOptions ++= Seq("-feature")
  )

  val commonDeps = Seq (
    "com.typesafe.akka"          %% "akka-actor"           % "2.3.2",
    "com.typesafe.scala-logging" %% "scala-logging-slf4j"  % "2.0.4",
    "org.scala-lang.modules"      % "scala-xml_2.11.0-RC1" % "1.0.0",
    "org.scalatest"              %% "scalatest"            % "2.1.3" % "test"
  )

  val mkrun = TaskKey[File]("mkrun")
  val mkexamples = TaskKey[File]("mkexamples")
  lazy val root = Project (
    "root",
    file(".")
  ) aggregate(core, examples)

  lazy val core = Project (
    "core",
    file("core"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= commonDeps,
      javaOptions += "-Xss64M",
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

        experimentOut
      }
    )
  ) dependsOn(core)
}
