import java.io.BufferedInputStream
import java.nio.file.Files

import CommonSettings.autoImport.network
import WavesNodeArtifactsPlugin.autoImport.wavesNodeVersion
import com.typesafe.sbt.SbtNativePackager.Universal
import com.typesafe.sbt.packager.Keys.{debianPackageDependencies, maintainerScripts, packageName}
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging.autoImport.maintainerScriptsAppend
import com.typesafe.sbt.packager.debian.DebianPlugin.Names.Postinst
import com.typesafe.sbt.packager.debian.DebianPlugin.autoImport.Debian
import com.typesafe.sbt.packager.debian.JDebPackaging
import com.typesafe.sbt.packager.linux.LinuxPackageMapping
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport.{defaultLinuxInstallLocation, linuxPackageMappings}
import com.typesafe.sbt.packager.linux.LinuxPlugin.{Users, mapGenericMappingsToLinux}
import com.typesafe.sbt.packager.universal.UniversalDeployPlugin
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveStreamFactory}
import sbt.Keys._
import sbt._

/**
  * @note Specify "maintainer" to solve DEB warnings
  */
object ExtensionPackaging extends AutoPlugin {

  object autoImport extends ExtensionKeys
  import autoImport._

  override def requires: Plugins = CommonSettings && UniversalDeployPlugin && JDebPackaging && WavesNodeArtifactsPlugin

  override def projectSettings: Seq[Def.Setting[_]] =
    Seq(
      packageDoc / publishArtifact := false,
      packageSrc / publishArtifact := false,
      Universal / javaOptions := Nil,
      // Here we record the classpath as it's added to the mappings separately, so
      // we can use its order to generate the bash/bat scripts.
      classpathOrdering := Nil,
      // Note: This is sometimes on the classpath via dependencyClasspath in Runtime.
      // We need to figure out why sometimes the Attributed[File] is correctly configured
      // and sometimes not.
      classpathOrdering += linkedProjectJar(
        jar = (Compile / packageBin).value,
        art = (Compile / packageBin / artifact).value,
        moduleId = projectID.value
      ),
      classpathOrdering ++= {
        val jar = """(.+)[-_]([\d\.]+.*)\.jar""".r
        val inDeb = filesInDeb((Compile / unmanagedBase).value / s"waves_${wavesNodeVersion.value}_all.deb")
          .filter(x => x.endsWith(".jar") && x.startsWith("./usr/share/waves/lib"))
          .map(_.split('/').last)
          .map {
            case jar(name, rev) => name -> rev
            case x              => throw new RuntimeException(s"Can't parse JAR name: $x")
          }
          .toMap

        val (r, conflicts) = excludeProvidedArtifacts((Runtime / dependencyClasspath).value, inDeb)
        streams.value.log.warn(s"Found conflicts (name: deb != ours):\n${conflicts.map { case (name, (deb, ours)) => s"$name: $deb != $ours" }.mkString("\n")}")
        r.toSeq
      },
      Universal / mappings ++= classpathOrdering.value ++ {
        val baseConfigName = s"${name.value}-${network.value}.conf"
        val localFile      = (Compile / baseDirectory).value / baseConfigName
        if (localFile.exists()) {
          val artifactPath = "doc/dex.conf.sample"
          Seq(localFile -> artifactPath)
        } else Seq.empty
      },
      classpath := makeRelativeClasspathNames(classpathOrdering.value),
      nodePackageName := s"waves${network.value.packageSuffix}",
      debianPackageDependencies := Seq(s"${nodePackageName.value} (= ${wavesNodeVersion.value})"),
      // To write files to Waves NODE directory
      linuxPackageMappings := getUniversalFolderMappings(
        nodePackageName.value,
        defaultLinuxInstallLocation.value,
        (Universal / mappings).value
      ),
      Debian / maintainerScripts := maintainerScriptsAppend((Debian / maintainerScripts).value - Postinst)(
        Postinst ->
          s"""#!/bin/sh
             |set -e
             |chown -R ${nodePackageName.value}:${nodePackageName.value} /usr/share/${nodePackageName.value}""".stripMargin
      )
    ) ++ nameFix ++ inScope(Global)(Seq(Global / name := (ThisProject / name).value) ++ nameFix)

  private def nameFix = Seq(
    packageName := s"${name.value}${network.value.packageSuffix}",
    normalizedName := s"${packageName.value}"
  )

  // A copy of com.typesafe.sbt.packager.linux.LinuxPlugin.getUniversalFolderMappings
  private def getUniversalFolderMappings(pkg: String, installLocation: String, mappings: Seq[(File, String)]): Seq[LinuxPackageMapping] = {
    def isWindowsFile(f: (File, String)): Boolean = f._2 endsWith ".bat"

    val filtered = mappings.filterNot(isWindowsFile)
    if (filtered.isEmpty) Seq.empty
    else mapGenericMappingsToLinux(filtered, Users.Root, Users.Root)(name => installLocation + "/" + pkg + "/" + name)
  }

  private def makeRelativeClasspathNames(mappings: Seq[(File, String)]): Seq[String] =
    for {
      (_, name) <- mappings
    } yield {
      // Here we want the name relative to the lib/ folder...
      // For now we just cheat...
      if (name startsWith "lib/") name drop 4
      else "../" + name
    }

  def linkedProjectJar(jar: File, art: Artifact, moduleId: ModuleID): (File, String) = {
    val jarName = ExtensionPackaging.makeJarName(
      org = moduleId.organization,
      name = moduleId.name,
      revision = moduleId.revision,
      artifactName = art.name,
      artifactClassifier = art.classifier
    )
    jar -> s"lib/$jarName"
  }

  /**
    * Constructs a jar name from components...(ModuleID/Artifact)
    */
  def makeJarName(org: String, name: String, revision: String, artifactName: String, artifactClassifier: Option[String]): String =
    org + "." +
      name + "-" +
      Option(artifactName.replace(name, "")).filterNot(_.isEmpty).map(_ + "-").getOrElse("") +
      revision +
      artifactClassifier.filterNot(_.isEmpty).map("-" + _).getOrElse("") +
      ".jar"

  // Determines a nicer filename for an attributed jar file, using the
  // ivy metadata if available.
  private def getJarFullFilename(dep: Attributed[File]): String = {
    val filename: Option[String] = for {
      module <- dep.metadata
      // sbt 0.13.x key
        .get(AttributeKey[ModuleID]("module-id"))
        // sbt 1.x key
        .orElse(dep.metadata.get(AttributeKey[ModuleID]("moduleID")))
      artifact <- dep.metadata.get(AttributeKey[Artifact]("artifact"))
    } yield makeJarName(module.organization, module.name, module.revision, artifact.name, artifact.classifier)
    filename.getOrElse(dep.data.getName)
  }

  private def excludeProvidedArtifacts(runtimeClasspath: Classpath,
                                       exclusions: Map[String, String]): (Map[File, String], Map[String, (String, String)]) = {
    val initJarMapping = Map.empty[File, String]
    val initConflicts  = Map.empty[String, (String, String)]
    runtimeClasspath.foldLeft((initJarMapping, initConflicts)) {
      case (r @ (jarMapping, conflicts), x) if x.data.isFile =>
        x.get(Keys.moduleID.key) match {
          case None => r
          case Some(artifact) =>
            val name = s"${artifact.organization}.${artifact.name}"
            exclusions.get(name) match {
              case None => (jarMapping.updated(x.data, "lib/" + getJarFullFilename(x)), conflicts)
              case Some(debRevision) =>
                if (debRevision == artifact.revision) r
                else (jarMapping, conflicts.updated(name, (debRevision, artifact.revision)))
            }
        }
      case (r, _) => r
    }
  }

  private def filesInDeb(file: File): List[String] = {
    val fs      = new BufferedInputStream(Files.newInputStream(file.toPath))
    val factory = new ArchiveStreamFactory()

    def entries: Iterator[ArchiveEntry] = {
      val ais = factory.createArchiveInputStream(fs)
      Iterator.continually(ais.getNextEntry).takeWhile(_ != null).filter(ais.canReadEntryData)
    }

    try entries
      .flatMap { x =>
        if (x.getName == "data.tar") entries else Iterator(x)
      }
      .map(_.getName)
      .toList
    finally fs.close()
  }
}
