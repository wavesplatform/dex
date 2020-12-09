package com.wavesplatform.dex.grpc.integration.clients.domain

import java.nio.charset.StandardCharsets

import cats.Monoid
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.semigroup._
import com.wavesplatform.dex.domain.account.KeyPair
import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.test.matchers.ProduceError.produce
import com.wavesplatform.dex.{NoShrink, WavesIntegrationSuiteBase}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.util.matching.Regex

class WavesChainTestSuite extends WavesIntegrationSuiteBase with ScalaCheckDrivenPropertyChecks with NoShrink {

  private val alice = KeyPair(ByteStr("alice".getBytes(StandardCharsets.UTF_8))).toAddress
  private val bob = KeyPair(ByteStr("bob".getBytes(StandardCharsets.UTF_8))).toAddress

  private val usd = IssuedAsset(Base58.decode("usd"))

  private val block1 = WavesBlock(
    ref = BlockRef(height = 1, id = ByteStr(Array[Byte](98, 0))),
    reference = ByteStr.empty,
    changes = BlockchainBalance(
      regular = Map(alice -> Map(Waves -> 10, usd -> 2L)),
      outLeases = Map(bob -> 23L)
    ),
    tpe = WavesBlock.Type.FullBlock
  )

  private val block2 = WavesBlock(
    ref = BlockRef(height = 2, id = ByteStr(Array[Byte](98, 1, 0))),
    reference = block1.ref.id,
    changes = BlockchainBalance(
      regular = Map(bob -> Map(usd -> 35)),
      outLeases = Map.empty
    ),
    tpe = WavesBlock.Type.FullBlock
  )

  private val emptyChain = Vector.empty[WavesBlock]

  "WavesChain" - {
    "dropLiquidBlock" - {
      "empty" in {
        WavesChain.dropLiquidBlock(block1, emptyChain) should matchTo((List.empty[WavesBlock], emptyChain))
      }

      "positive" - {
        def testGen(blocksNumber: Range, microBlocksNumber: Range): Gen[(WavesBlock, Vector[WavesBlock])] =
          historyGen(blocksNumber, microBlocksNumber).map { history =>
            val headBlock = history.headOption
            (
              WavesBlock(
                ref = BlockRef(headBlock.fold(1)(_.ref.height + 1), ByteStr(Array[Byte](-7))),
                reference = headBlock.fold(ByteStr(Array[Byte](-8)))(_.ref.id),
                changes = Monoid.empty[BlockchainBalance],
                tpe = WavesBlock.Type.FullBlock
              ),
              history
            )
          }

        "liquid block is not empty" - {
          "the last liquid block's part is referenced by a new block" in forAll(testGen(1 to 3, 1 to 3)) { case (newBlock, history) =>
            val (liquidBlock, _) = WavesChain.dropLiquidBlock(newBlock, history)
            liquidBlock.last.ref.id shouldBe newBlock.reference
          }

          "the rest history should not contain a referenced block" in forAll(testGen(1 to 3, 1 to 3)) { case (newBlock, history) =>
            val (_, restHistory) = WavesChain.dropLiquidBlock(newBlock, history)
            restHistory.find(_.ref.id == newBlock.reference) shouldBe empty
          }
        }

        "liquid block is empty" - {
          "when there are no micro blocks" in forAll(testGen(1 to 3, 0 to 0)) { case (newBlock, history) =>
            val (liquidBlock, _) = WavesChain.dropLiquidBlock(newBlock, history)
            liquidBlock shouldBe empty
          }

          "the head block in the rest history is referenced by a new one" in forAll(testGen(1 to 3, 0 to 0)) { case (newBlock, history) =>
            val (_, restHistory) = WavesChain.dropLiquidBlock(newBlock, history)
            restHistory.head.ref.id shouldBe newBlock.reference
          }
        }
      }
    }

    "dropDifference" - {
      val testGen = for {
        commonBlocks <- historyGen(0 to 2, 0 to 2)
        (maxBlocksNumber, startHeight) = commonBlocks.headOption match {
          case Some(lastBlock) =>
            if (lastBlock.tpe == WavesBlock.Type.FullBlock) (2, lastBlock.ref.height + 1)
            else (0, lastBlock.ref.height)
          case _ => (2, 0)
        }

        detachedHistory1 <- historyGen(0 to maxBlocksNumber, 0 to 2, startHeight to startHeight)
        detachedHistory2 <- historyGen(0 to maxBlocksNumber, 0 to 2, startHeight to startHeight)
      } yield {
        val history1 = detachedHistory1.appendedAll(commonBlocks)
        val history2 = detachedHistory2.appendedAll(commonBlocks)
        (
          commonBlocks,
          WavesChain(history1, history1.headOption.fold(0)(_.ref.height), 100),
          WavesChain(history2, history2.headOption.fold(0)(_.ref.height), 100)
        )
      }

      "there is no common block between dropped" in forAll(testGen) { case (commonBlocks, chain1, chain2) =>
        val (dropped1, dropped2) = WavesChain.dropDifference(chain1, chain2)
        commonBlocks.foreach { commonBlock =>
          withClue("dropped1: ") {
            dropped1 should not contain commonBlock
          }
          withClue("dropped2: ") {
            dropped2 should not contain commonBlock
          }
        }
      }
    }

    // Terminology:
    // * block - full block or micro block
    // Properties:
    // 1. If we have no blocks and receive a full block, this block is added in the front of the current fork
    // 2. If we have no blocks and receive a micro block, we restart from the current height
    // 3. If we have no micro blocks and receive a full block that is referenced to the last known block, this block is added in the front of the current fork
    // 4. If we have micro blocks and receive a full block that is referenced to the last known full block or its micro blocks
    //    1. The last block and its micro blocks are replaced by a hardened block in the fork
    //    2. See 3
    // 5. If we receive an irrelevant block, we drop the last full block and its micro blocks [and restart the stream from the last block's height]

    "withBlock" - {
      "properties" - {
        val testGen: Gen[(WavesChain, WavesBlock)] =
          Gen.oneOf(
            historyGen(0 to 2, 0 to 2, 0 to 2).map { history =>
              val newBlock = mkNextFullBlock(if (history.isEmpty) defaultInitBlock else history.head)
              (WavesChain(history, history.headOption.fold(0)(_.ref.height), 100), newBlock)
            },
            historyGen(1 to 2, 0 to 2, 0 to 2).map { history =>
              val newBlock = mkNextMicroBlock(history.head)
              (WavesChain(history, history.headOption.fold(0)(_.ref.height), 100), newBlock)
            }
          )

        def test(f: (WavesChain, WavesChain, WavesBlock) => Any): Any = forAll(testGen) { case (chain, newBlock) =>
          chain.withBlock(newBlock) match {
            case Left(e) => fail(e)
            case Right(updatedChain) => f(chain, updatedChain, newBlock)
          }
        }

        "a new block is the last block in the chain after appending" in test { (_, updatedChain, newBlock) =>
          updatedChain.last should matchTo(newBlock.some)
        }

        "the height increased if we append a full block" in test { (chain, updatedChain, newBlock) =>
          val expectedHeight = chain.height + (if (newBlock.tpe == WavesBlock.Type.FullBlock) 1 else 0)
          updatedChain.height shouldBe expectedHeight
        }

        "the capacity decreased if we append a full block" in test { (chain, updatedChain, newBlock) =>
          val expectedCapacity = chain.blocksCapacity - (if (newBlock.tpe == WavesBlock.Type.FullBlock) 1 else 0)
          updatedChain.blocksCapacity shouldBe expectedCapacity
        }

        "the length preserved if we append a block and the capacity exhausted" in {
          val testGen = for {
            history <- historyGen(1 to 2, 0 to 0, 0 to 2)
          } yield {
            val chain = WavesChain(history, history.head.ref.height, 0)
            (chain, mkNextFullBlock(history.head))
          }

          forAll(testGen) { case (chain, newBlock) =>
            chain.withBlock(newBlock) match {
              case Left(e) => fail(e)
              case Right(updatedChain) => updatedChain.history.length shouldBe chain.history.length
            }
          }
        }

        "the length increased if we append a micro block even the capacity exhausted" in {
          val testGen = for {
            history <- historyGen(1 to 2, 0 to 2, 0 to 2)
          } yield {
            val chain = WavesChain(history, history.head.ref.height, 0)
            (chain, mkNextMicroBlock(history.head))
          }

          forAll(testGen) { case (chain, newBlock) =>
            chain.withBlock(newBlock) match {
              case Left(e) => fail(e)
              case Right(updatedChain) => updatedChain.history.length shouldBe (chain.history.length + 1)
            }
          }
        }
      }

      "empty +" - {
        val init = WavesChain(emptyChain, 0, 100)

        "block" in { init.withBlock(block1) should matchTo(WavesChain(Vector(block1), 1, 99).asRight[String]) }

        "micro block" in {
          val microBlock = block1.copy(tpe = WavesBlock.Type.MicroBlock)
          init.withBlock(microBlock) should produce("(?s)^Can't attach a micro block.+to empty chain$".r)
        }
      }

      "block +" - {
        val init = WavesChain(Vector(block1), 1, 99)
        "expected" - {
          "block" in { init.withBlock(block2) should matchTo(WavesChain(Vector(block2, block1), 2, 98).asRight[String]) }

          "micro block" in {
            val microBlock = block2.copy(
              ref = BlockRef(height = 1, id = block2.ref.id),
              tpe = WavesBlock.Type.MicroBlock
            )

            init.withBlock(microBlock) should matchTo(WavesChain(Vector(microBlock, block1), 1, 99).asRight[String])
          }
        }

        "unexpected" - {
          def test(message: Regex, updateNext: WavesBlock => WavesBlock): Unit = init.withBlock(updateNext(block2)) should produce(message)

          "block" - {
            "unexpected reference" in test("(?s)^The new block.+must be after.+".r, _.copy(reference = ByteStr.empty))

            "unexpected height" - {
              def heightTest(h: Int): Unit = test("(?s)^The new block.+must be after.+".r, x => x.copy(ref = x.ref.copy(height = h)))
              "1" in heightTest(1)
              "3" in heightTest(3)
            }
          }

          "micro block" - {
            "unexpected reference" in test(
              "(?s)^The new micro block.+must reference.+".r,
              _.copy(
                tpe = WavesBlock.Type.MicroBlock,
                reference = ByteStr.empty
              )
            )

            "unexpected height" - {
              def heightTest(h: Int): Unit = test(
                "(?s)^The new micro block.+must reference.+".r,
                x => x.copy(tpe = WavesBlock.Type.MicroBlock, ref = x.ref.copy(height = h))
              )
              "1" in heightTest(0)
              "3" in heightTest(2)
            }
          }
        }
      }

      "block, micro block +" - {
        val microBlock1 = WavesBlock(
          ref = BlockRef(height = 1, id = ByteStr(Array[Byte](98, 0, 1))),
          reference = block1.ref.id,
          changes = BlockchainBalance(
            regular = Map(bob -> Map(usd -> 7), alice -> Map(usd -> 24)),
            outLeases = Map.empty
          ),
          tpe = WavesBlock.Type.MicroBlock
        )

        val init = WavesChain(Vector(microBlock1, block1), 1, 99)

        "expected" - {
          "block referenced to the" - {
            "micro block" in {
              val newBlock = WavesBlock(
                ref = BlockRef(height = 2, id = ByteStr(Array[Byte](98, 1))),
                reference = microBlock1.ref.id,
                changes = BlockchainBalance(
                  regular = Map(alice -> Map(Waves -> 9), bob -> Map(usd -> 2L)),
                  outLeases = Map(alice -> 1L)
                ),
                tpe = WavesBlock.Type.FullBlock
              )

              val hardenedBlock = block1.copy(
                ref = microBlock1.ref,
                reference = block1.reference,
                changes = block1.changes |+| microBlock1.changes,
                tpe = WavesBlock.Type.FullBlock
              )

              init.withBlock(newBlock) should matchTo(WavesChain(Vector(newBlock, hardenedBlock), 2, 98).asRight[String])
            }
          }

          "micro block" in {
            val microBlock2 = block2.copy(
              ref = BlockRef(height = 1, id = ByteStr(Array[Byte](98, 0, 2))),
              reference = microBlock1.ref.id,
              tpe = WavesBlock.Type.MicroBlock
            )

            init.withBlock(microBlock2) should matchTo(WavesChain(Vector(microBlock2, microBlock1, block1), 1, 99).asRight[String])
          }
        }

        "unexpected" - {
          def test(message: Regex, updateNext: WavesBlock => WavesBlock): Unit =
            init.withBlock(updateNext(block2)) should produce(message)

          "block" - {
            "unexpected reference" in test("(?s)^The new block.+must be after.+".r, _.copy(reference = ByteStr.empty))

            "unexpected height" - {
              def heightTest(h: Int): Unit = test("(?s)^The new block.+must be after.+".r, x => x.copy(ref = x.ref.copy(height = h)))
              "1" in heightTest(1)
              "3" in heightTest(3)
            }

            "referenced to a key block" in {
              val newBlock = WavesBlock(
                ref = BlockRef(height = 2, id = ByteStr(Array[Byte](98, 1))),
                reference = block1.ref.id,
                changes = BlockchainBalance( // TODO changes here are not essential
                  regular = Map(alice -> Map(Waves -> 9), bob -> Map(usd -> 2L)),
                  outLeases = Map(alice -> 1L)
                ),
                tpe = WavesBlock.Type.FullBlock
              )

              init.withBlock(newBlock) should produce("(?s)^The new block.+must be after.+".r)
            }
          }

          "micro block" - {
            "unexpected reference" in test(
              "(?s)^The new micro block.+must reference.+".r,
              _.copy(
                tpe = WavesBlock.Type.MicroBlock,
                reference = ByteStr.empty
              )
            )

            "unexpected height" - {
              def heightTest(h: Int): Unit = test(
                "(?s)^The new micro block.+must reference.+".r,
                x => x.copy(tpe = WavesBlock.Type.MicroBlock, ref = x.ref.copy(height = h))
              )
              "1" in heightTest(0)
              "3" in heightTest(2)
            }
          }
        }
      }

      "block, micro block, micro block +" - {
        val microBlock1 = WavesBlock(
          ref = BlockRef(height = 1, id = ByteStr(Array[Byte](98, 0, 1))),
          reference = block1.ref.id,
          changes = BlockchainBalance(
            regular = Map(bob -> Map(usd -> 7), alice -> Map(usd -> 24)),
            outLeases = Map.empty
          ),
          tpe = WavesBlock.Type.MicroBlock
        )

        val microBlock2 = WavesBlock(
          ref = BlockRef(height = 1, id = ByteStr(Array[Byte](98, 0, 2))),
          reference = microBlock1.ref.id,
          changes = BlockchainBalance(
            regular = Map(bob -> Map(usd -> 3), alice -> Map(usd -> 11)),
            outLeases = Map.empty
          ),
          tpe = WavesBlock.Type.MicroBlock
        )

        val init = WavesChain(Vector(microBlock2, microBlock1, block1), 1, 99)

        "unexpected" - {
          "block referenced to the previous micro block" in {
            val newBlock = WavesBlock(
              ref = BlockRef(height = 2, id = ByteStr(Array[Byte](98, 2))),
              reference = microBlock1.ref.id,
              changes = BlockchainBalance(
                regular = Map(alice -> Map(Waves -> 9), bob -> Map(usd -> 2L)),
                outLeases = Map(alice -> 1L)
              ),
              tpe = WavesBlock.Type.FullBlock
            )

            init.withBlock(newBlock) should produce("(?s)^The new block.+must be after.+".r)
          }

          "micro block referenced to the previous micro block" - {
            "unexpected reference" in {
              val microBlock3 = WavesBlock(
                ref = BlockRef(height = 1, id = ByteStr(Array[Byte](98, 0, 3))),
                reference = microBlock1.ref.id,
                changes = BlockchainBalance(
                  regular = Map(bob -> Map(usd -> 3), alice -> Map(usd -> 11)),
                  outLeases = Map.empty
                ),
                tpe = WavesBlock.Type.MicroBlock
              )

              init.withBlock(microBlock3) should produce("(?s)^The new micro block.+must reference the last block.+".r)
            }
          }
        }
      }

      "block, block, micro block + block referenced to the previous one" in {
        val microBlock = WavesBlock(
          ref = BlockRef(height = 2, id = ByteStr(Array[Byte](98, 0, 1))),
          reference = block2.ref.id,
          changes = BlockchainBalance(
            regular = Map(bob -> Map(usd -> 7), alice -> Map(usd -> 24)),
            outLeases = Map.empty
          ),
          tpe = WavesBlock.Type.MicroBlock
        )

        val init = WavesChain(Vector(microBlock, block2, block1), 2, 98)

        val newBlock = WavesBlock(
          ref = BlockRef(height = 3, id = ByteStr(Array[Byte](98, 1, 0))),
          reference = block1.ref.id,
          changes = BlockchainBalance(
            regular = Map(bob -> Map(usd -> 35)),
            outLeases = Map.empty
          ),
          tpe = WavesBlock.Type.FullBlock
        )

        init.withBlock(newBlock) should produce("(?s)^The new block.+must be after.+".r)
      }
    }

    "withoutLast" - {
      def testGen(maxMicroBlocks: Range = 0 to 2): Gen[WavesChain] = for {
        history <- historyGen(1 to 2, maxMicroBlocks, 0 to 2)
        capacity <- Gen.choose(0, 2)
      } yield WavesChain(history, history.head.ref.height, capacity)

      "the last block disappears" in forAll(testGen()) { chain =>
        val (updatedChain, _) = chain.withoutLast
        updatedChain.history should not contain chain.last.get
      }

      "the capacity increases if the last block is a full block" in forAll(testGen(0 to 0)) { chain =>
        val (updatedChain, _) = chain.withoutLast
        updatedChain.blocksCapacity shouldBe chain.blocksCapacity + 1
      }

      "the capacity remains if the last block is a micro block" in forAll(testGen(1 to 2)) { chain =>
        val (updatedChain, _) = chain.withoutLast
        updatedChain.blocksCapacity shouldBe chain.blocksCapacity
      }
    }
  }

  private lazy val defaultInitBlock = WavesBlock(
    ref = BlockRef(0, ByteStr(Array[Byte](0))),
    reference = ByteStr.empty,
    changes = Monoid.empty[BlockchainBalance],
    tpe = WavesBlock.Type.FullBlock
  )

  private def historyGen(blocksNumber: Range, microBlocksNumber: Range, startHeightRange: Range = 0 to 2): Gen[Vector[WavesBlock]] =
    for {
      startHeight <- Gen.choose(startHeightRange.head, startHeightRange.last)
      r <- historyGen(
        blocksNumber,
        microBlocksNumber,
        initBlock = defaultInitBlock.copy(
          ref = BlockRef(
            height = startHeight,
            id = ByteStr(Array.fill[Byte](startHeight)(1))
          )
        )
      )
    } yield r

  private def historyGen(blocksNumber: Range, microBlocksNumber: Range, initBlock: WavesBlock): Gen[Vector[WavesBlock]] = for {
    blocksNumber <- Gen.choose(blocksNumber.head, blocksNumber.last)
    microBlocksNumber <- if (blocksNumber == 0) Gen.const(0) else Gen.choose(microBlocksNumber.head, microBlocksNumber.last)
  } yield historyGen(blocksNumber, microBlocksNumber, initBlock)

  private def historyGen(blocksNumber: Int, microBlocksNumber: Int, initBlock: WavesBlock): Vector[WavesBlock] =
    if (blocksNumber <= 0) Vector.empty
    else {
      val blocks = {
        Iterator(initBlock) ++ Iterator
          .unfold(initBlock) { prev =>
            val next = mkNextFullBlock(prev)
            (next, next).some
          }
      }
        .take(blocksNumber)
        .toVector // 1, 2, 3

      val microBlocks = blocks.lastOption match {
        case None => Vector.empty[WavesBlock]
        case Some(lastBlock) =>
          Iterator
            .unfold(lastBlock) { prev =>
              val next = mkNextMicroBlock(prev)
              (next, next).some
            }
            .take(microBlocksNumber)
            .toVector // 4, 5
      }

      blocks.appendedAll(microBlocks).reverse // 5, 4, 3, 2, 1
    }

  private def mkNextFullBlock(prevBlock: WavesBlock): WavesBlock = prevBlock.copy(
    ref = BlockRef(prevBlock.ref.height + 1, ByteStr(prevBlock.ref.id.arr.prepended(1))),
    reference = prevBlock.ref.id,
    tpe = WavesBlock.Type.FullBlock
  )

  private def mkNextMicroBlock(prevBlock: WavesBlock): WavesBlock = prevBlock.copy(
    ref = prevBlock.ref.copy(id = ByteStr(prevBlock.ref.id.arr.prepended(2))), // height remains
    reference = prevBlock.ref.id,
    tpe = WavesBlock.Type.MicroBlock
  )

}
