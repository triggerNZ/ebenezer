package com.cba.omnia.ebenezer
package fs

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

object Glob {
  def patterns(conf: Configuration, patterns: List[String]): List[Path] =
    paths(conf, patterns.map(new Path(_)))

  def paths(conf: Configuration, paths: List[Path]): List[Path] = {
    val fs = FileSystem.get(conf)
    paths.flatMap(path => fs.globStatus(path).toList.map(_.getPath))
  }
}
