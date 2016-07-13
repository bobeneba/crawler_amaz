name := "testament"

organization := "yks.be.datamagic"

version := "1.0.0"

scalaVersion := "2.10.3"

credentials += Credentials(Path.userHome / ".sbt" / "sonatype.credentials")

libraryDependencies +="org.apache.kafka"%"kafka_2.10"%"0.8.1.1"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.7"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.15"

libraryDependencies += "commons-httpclient" % "commons-httpclient" % "3.1"

libraryDependencies +="yks.be.datamagic" % "spider-man_2.10" % "1.0.0-SNAPSHOT" 


libraryDependencies += "org.avaje" % "ebean" % "2.7.3"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.34"

libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"

libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

resolvers ++= Seq(  

"snapshots" at "http://ci.nexus.kokoerp.com:8081/nexus/content/repositories/snapshots",

"releases" at "http://ci.nexus.kokoerp.com:8081/nexus/content/repositories/releases",

"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",

"Akka Repository" at "http://repo.akaka.io/releases/"
)

publishTo := {
  val nexus = "http://ci.nexus.kokoerp.com:8081/nexus/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots") 
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}



