package ru.delimobil

import cats.MonadThrow
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.applicative._
import cats.syntax.option._
import fs2.Chunk
import fs2.Pure
import fs2.Stream

import scala.concurrent.duration.Duration
import scala.concurrent.duration._

object FakeDP {

  case class MaybeZDT(zdt: Option[Int])

  val empty: MaybeZDT = MaybeZDT(none)

  val big = 2

  val small = 1

  final class FakeTrackToUpdateAction {
    def assemble(tracks: Stream[IO, MaybeZDT]): Stream[IO, MaybeZDT] =
      (Stream(empty) ++ tracks)
        .sliding(2)
        .map { chunk => MaybeZDT(chunk.head.get.zdt) }
        .chunks
        .flatMap(pickLastZdt)

    private def pickLastZdt(chunk: Chunk[MaybeZDT]): Stream[Pure, MaybeZDT] =
      Stream.fromOption(chunk.last).map(action => MaybeZDT(action.zdt))
  }

  final class FakeTrackTableToHBaseTransferService(
    trackToUpdateAction: FakeTrackToUpdateAction,
    parallelismPerTrack: Int,
    trackerTimeValidityThreshold: FiniteDuration,
  ) {

    def transferTracks: Stream[IO, Int] = {
      val track = empty.copy(zdt = big.some)
      Stream(track, track)
        .repeat
        .through(trackToUpdateAction.assemble)
        .evalTap(checkDateValidity)
        .parEvalMap(parallelismPerTrack)(_ => IO.pure(1))
    }

    private def checkDateValidity(updateAction: MaybeZDT): IO[Unit] = {
      val shouldTerminate = updateAction.zdt.exists(_ > small)
      MonadThrow[IO].raiseError(new IllegalArgumentException("TheException")).whenA(shouldTerminate)
    }
  }

  def make = {
    val transferService =
      new FakeTrackTableToHBaseTransferService(
        new FakeTrackToUpdateAction,
        parallelismPerTrack = 10,
        trackerTimeValidityThreshold = Duration.Zero,
      )

    val stream = transferService.transferTracks.handleErrorWith(ex => Stream.exec(IO.println(s"Caught ${ex.getMessage}")))

    Stream.emits((1 to 100).map(_ => empty))
      .flatMap(_ => stream)
      .groupWithin(512, 1.second)
      .compile
      .drain
  }
}

object FS2Example extends IOApp.Simple {
  def run: IO[Unit] = Stream.repeatEval(FakeDP.make).compile.drain
}
