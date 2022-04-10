//resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/sbt/sbt-plugin-releases/com.eed3si9n/sbt-assembly/scala_2.12/sbt_1.0/0.14.5/ivys/"))
resolvers += "bintray-sbt-plugins" at "https://dl.bintray.com/sbt/sbt-plugin-releases/com.eed3si9n/sbt-assembly/scala_2.12/sbt_1.0/0.14.5/ivys/"
//resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5" )

//addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.12")
