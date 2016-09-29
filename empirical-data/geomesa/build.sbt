resolvers ++= Seq(
  "locationtech" at "https://repo.locationtech.org/content/groups/releases",
  Resolver.bintrayRepo("azavea","geotrellis")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Version.spark % "provided",
  "org.apache.hadoop" % "hadoop-client" % Version.hadoop % "provided",
  "com.azavea.geotrellis" %% "geotrellis-spark" % "0.10.2",
  "org.apache.accumulo" % "accumulo-core" % "1.7.0",
  "org.locationtech.geomesa" % "geomesa-accumulo-datastore" % Version.geomesa
    excludeAll(ExclusionRule("org.slf4j")),
  "org.locationtech.geomesa" % "geomesa-compute" % Version.geomesa
    excludeAll(ExclusionRule("org.slf4j")),
  "org.locationtech.geomesa" % "geomesa-jobs" % Version.geomesa,
  "org.locationtech.geomesa" % "geomesa-utils" % Version.geomesa
    excludeAll(ExclusionRule("org.slf4j"))
)

excludeDependencies ++= Seq(
  SbtExclusionRule("com.typesafe.scala-logging", "scala-logging-api_2.11")
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.clearspring.analytics.**" -> "org.locationtech.geomesa.shaded.com.clearspring.analytics.@1").inAll
)

// When creating fat jar, remote some files with
// bad signatures and resolve conflicts by taking the first
// versions of shared packaged types.
assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
      // Concatenate everything in the services directory to keep GeoTools happy.
      case ("services" :: _ :: Nil) => MergeStrategy.concat
      // Concatenate these to keep JAI happy.
      case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) => {
        // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
          MergeStrategy.discard
        else
          MergeStrategy.first
      }
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
