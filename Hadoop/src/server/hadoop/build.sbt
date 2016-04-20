organization := "org.hadoop"
name := "hadoop"
version := "1.0"
libraryDependencies += "commons-io" % "commons-io" % "2.4"
libraryDependencies ++= Seq("com.amazonaws" % "aws-java-sdk-s3" % "1.10.47",
							"com.amazonaws" % "aws-java-sdk-ec2" % "1.10.69",
                            "commons-lang" % "commons-lang" % "2.6")
libraryDependencies +="com.github.NatTuck" % "textsock" % "0.1.0-SNAPSHOT"
                            


