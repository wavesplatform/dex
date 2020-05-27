package com.wavesplatform.dex

import java.io.{PrintWriter, StringWriter}

import cats.syntax.either._

package object tool {

  type ErrorOr[A] = Either[String, A]

  def lift[A](a: A): ErrorOr[A] = a.asRight

  val success: ErrorOr[Unit] = lift { () }

  def log(log: String, indent: Option[Int] = None): ErrorOr[Unit] = lift {
    indent.fold { print(log) } { i =>
      print(log + " " * (i - log.length))
    }
  }

  def wrapByLogs[A](f: => ErrorOr[A])(begin: String, end: String, indent: Option[Int] = None): ErrorOr[A] =
    for {
      _      <- log(begin, indent)
      result <- f
      _      <- log(end)
    } yield result

  implicit class ThrowableOps(private val t: Throwable) extends AnyVal {

    def getWithStackTrace: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      s"$t, ${sw.toString}"
    }
  }
}
