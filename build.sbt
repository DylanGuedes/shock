lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ime.usp.br",
      scalaVersion := "2.11.8"
    )),
    name := "shock",
    version := "0.0.1",
    sparkVersion := "2.2.0",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    sparkComponents := Seq("core", "sql", "catalyst", "mllib"),
    parallelExecution in Test := false,
    fork := true,
    coverageHighlighting := true,
    libraryDependencies ++= Seq(
      "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0"),

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },
    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),
    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )

libraryDependencies += "org.jfarcand" % "wcs" % "1.5"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.8"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.2.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.1"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"
