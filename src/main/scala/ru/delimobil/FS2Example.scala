package ru.delimobil

import cats.effect.IO
import cats.effect.IOApp
import fs2.Stream

import scala.concurrent.duration._

object FS2Example extends IOApp.Simple {

  val err = new IllegalArgumentException("TheException")

  def run: IO[Unit] = {
    val singleRun =
      Stream(()).concurrently((Stream(()) ++ Stream.raiseError[IO](err)).repeat)
        .handleErrorWith(ex => Stream.exec(IO.println(s"Caught ${ex.getMessage}")))
        .groupWithin(512, 1.second)

    Stream.repeatEval(singleRun.compile.drain).compile.drain.guaranteeCase(IO.println)
  }
}
