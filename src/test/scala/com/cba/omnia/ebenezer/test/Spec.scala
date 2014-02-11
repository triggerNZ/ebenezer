package com.cba.omnia.ebenezer
package test

import com.twitter.scalding._

import org.specs2._
import org.specs2.matcher._


trait Spec extends Specification
    with TerminationMatchers
    with ThrownExpectations
    with ScalaCheck {

  def run: Matcher[JobTest] =
    (j: JobTest) => { j.run.runHadoop; ok }

  def runInMemory: Matcher[JobTest] =
    (j: JobTest) => { j.run; ok }

  def runHadoop: Matcher[JobTest] =
    (j: JobTest) => { j.runHadoop; ok }

}
