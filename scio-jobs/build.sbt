import sbt._
import Keys._

val scioVersion = "0.7.4"
val beamVersion = "2.11.0"
val scalaMacrosVersion = "2.1.1"
val circeVersion = "0.11.0"
val sttpVersion = "1.5.10"
val googleCloudVersion = "1.28.0"

assemblyJarName := "scio-jobs.jar"

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly ~= { old =>
    {
      case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
      case s if s.endsWith("public-suffix-list.txt") =>
        MergeStrategy.filterDistinctLines
      case s if s.endsWith(".class") => MergeStrategy.last
      case s if s.endsWith(".proto") => MergeStrategy.last
      case s                         => old(s)
    }
  }
)

addCommandAlias("fmt", ";scalafmt;test:scalafmt;scalafmtSbt")
addCommandAlias("version", "sbtVersion")

initialize ~= { _ =>
  {
    System.setProperty("bigquery.cache.enabled", "false")
    System.setProperty("bigquery.priority", "INTERACTIVE")
  }
}

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "cityblock",
  version := "1.0.0",
  scalaVersion := "2.12.8",
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Yrangepos",
    "-Ywarn-unused"
  ),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = Project("Mixer", file("."))
  .settings(assemblySettings)
  .settings(
    name := "scio-jobs",
    commonSettings ++ macroSettings ++ noPublishSettings,
    resolvers += Resolver.sonatypeRepo("releases"),
    resolvers += Resolver.sonatypeRepo("snapshots"),
    description := "All platform related code",
    fork in run := false,
    libraryDependencies ++= Seq(
      // optional direct runner
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "com.spotify" %% "scio-bigquery" % scioVersion,
      "com.github.melrief" %% "purecsv" % "0.1.1",
      "org.slf4j" % "slf4j-simple" % "1.7.25"
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser",
      "io.circe" %% "circe-optics",
      "io.circe" %% "circe-java8",
      "io.circe" %% "circe-generic-extras"
    ).map(_ % circeVersion),
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp" %% "core" % sttpVersion,
      "com.softwaremill.sttp" %% "circe" % sttpVersion,
      "com.softwaremill.sttp" %% "okhttp-backend" % sttpVersion
    ),
    libraryDependencies += "com.google.api-client" % "google-api-client" % googleCloudVersion,
    libraryDependencies += "com.google.cloud" % "google-cloud" % "0.47.0-alpha",
    libraryDependencies += "com.google.cloud" % "google-cloud-kms" % googleCloudVersion,
    libraryDependencies += "com.google.cloud" % "google-cloud-secretmanager" % "0.1.0",
    libraryDependencies += "io.argonaut" %% "argonaut" % "6.2.2",
    libraryDependencies += "com.typesafe" % "config" % "1.3.2",
    libraryDependencies += "org.typelevel" %% "squants" % "1.3.0",
    libraryDependencies += "com.nrinaudo" %% "kantan.xpath" % "0.5.0",
    libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.5.1",
    libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.1",
    libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.1",
    libraryDependencies += "com.jsuereth" %% "scala-arm" % "2.0",
    libraryDependencies += "com.github.vital-software" %% "scala-redox" % "8.0.3",
    libraryDependencies += "com.github.julien-truffaut" %% "monocle-macro" % "1.4.0",
    libraryDependencies += "org.jsoup" % "jsoup" % "1.11.2",
    libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test,
    scalafmtOnCompile := true
  )
  .enablePlugins(PackPlugin)

lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(noPublishSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for Mixer",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    mainClass in Compile := Some("com.spotify.scio.repl.ScioShell")
  )
  .dependsOn(
    root
  )

lazy val computedFields = Map(
  "ResendJob" -> "cityblock.computedfields.common.ResendJob",
  "NewProductionResults" -> "cityblock.computedfields.jobs.NewProductionResults"
)

lazy val importers = Map(
  "DatabaseMirroringRunner" -> "cityblock.importers.mirroring.DatabaseMirroringRunner",
  "LoadEmblemPBM" -> "cityblock.importers.emblem.LoadPBM",
  "PublishMemberAttributionData" -> "cityblock.importers.common.PublishMemberAttributionData",
  "AttributionRunner" -> "cityblock.member.attribution.AttributionRunner",
  "PatientIndexImporter" -> "cityblock.importers.common.PatientIndexImporter",
  "LoadToSilverRunner" -> "cityblock.importers.generic.LoadToSilverRunner",
  "EmblemResolver" -> "cityblock.member.resolution.emblem.EmblemRunner"
)

lazy val transformers = Map(
  "PolishEmblemClaimsDataV2" -> "cityblock.transforms.emblem.PolishEmblemClaimsCohortV2",
  "PolishCCIClaimsData" -> "cityblock.transforms.connecticare.PolishCCIClaims",
  "PolishCCIFacetsMonthlyData" -> "cityblock.transforms.connecticare.PolishCCIFacetsMonthlyData",
  "PolishCCIFacetsWeeklyData" -> "cityblock.transforms.connecticare.PolishCCIFacetsWeeklyData",
  "PolishTuftsData" -> "cityblock.transforms.tufts.PolishTuftsData",
  "PolishTuftsDataDaily" -> "cityblock.transforms.tufts.PolishDailyTuftsData",
  "PushElationPatientEncounters" -> "cityblock.aggregators.jobs.PushElationPatientEncounters",
  "ClinicalSummaryRefresher" -> "cityblock.aggregators.jobs.ClinicalSummaryRefresher",
  "PolishCareFirstWeeklyData" -> "cityblock.transforms.carefirst.PolishCarefirstWeeklyClaims",
  "PolishCarefirstFullRefreshClaims" -> "cityblock.transforms.carefirst.PolishCarefirstFullRefreshClaims",
  "PriorAuthToHIETransformer" -> "cityblock.transforms.emblem.gold.PriorAuthToHIETransformer"
)

lazy val combiners = Map(
  "CombineCCIClaims" -> "cityblock.transforms.connecticare.CombineCCIClaims",
  "CombineTuftsData" -> "cityblock.transforms.tufts.CombineTuftsData",
  "CombineReplaceData" -> "cityblock.transforms.combine.CombineReplaceData"
)

lazy val batch = Map(
  "PushPatientClaimsEncounters" -> "cityblock.aggregators.jobs.PushPatientClaimsEncounters",
  "PushPatientEligibilities" -> "cityblock.aggregators.jobs.PushPatientEligibilities",
  "AggregatePatientClaimsEncounters" -> "cityblock.aggregators.jobs.PatientClaimsEncounters"
)

packMain := (
  computedFields
  ++ importers
  ++ transformers
  ++ batch
  ++ combiners
)
packResourceDir += (baseDirectory.value / "src/main/resources/" -> "")
