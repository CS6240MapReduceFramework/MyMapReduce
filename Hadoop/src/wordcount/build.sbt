mainClass in Compile := Some("main.WordCount")
libraryDependencies += "commons-io" % "commons-io" % "2.4"
libraryDependencies ++= Seq("com.amazonaws" % "aws-java-sdk-s3" % "1.10.47",
                            "commons-lang" % "commons-lang" % "2.6")

libraryDependencies += "com.github.NatTuck" % "hadoop" % "0.1.0-SNAPSHOT"

