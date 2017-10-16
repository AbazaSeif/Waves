package scorex.lagonaki.unit

import com.wavesplatform.TransactionGen
import com.wavesplatform.metrics.Instrumented
import org.scalatest.prop.PropertyChecks
import org.scalatest._
import com.wavesplatform.state2.diffs.produce
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Gen, Shrink}
import scorex.block.Block
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.crypto.EllipticCurveImpl
import scorex.crypto.hash.FastCryptographicHash
import scorex.transaction._
import com.wavesplatform.state2._

class BlockSpecification extends PropSpec with PropertyChecks with TransactionGen with Matchers {

  private implicit def noShrink[A]: Shrink[A] = Shrink(_ => Stream.empty)

  val time = System.currentTimeMillis() - 5000

  val blockGen = for {
    baseTarget <- arbitrary[Long]
    reference <- byteArrayGen(Block.BlockIdLength).map(r => ByteStr(r))
    generationSignature <- byteArrayGen(Block.GeneratorSignatureLength)
    assetBytes <- byteArrayGen(AssetIdLength)
    assetId = Some(ByteStr(assetBytes))
    sender <- accountGen
    recipient <- accountGen
    paymentTransaction <- paymentGeneratorP(time, sender, recipient)
    transferTrancation <- transferGeneratorP(1 + time, sender, recipient, assetId, None)
    anotherPaymentTransaction <- paymentGeneratorP(2 + time, sender, recipient)
    transactionData = Seq(paymentTransaction, transferTrancation, anotherPaymentTransaction)
  } yield (baseTarget, reference, ByteStr(generationSignature), recipient, transactionData)

  def bigBlockGen(amt: Int): Gen[Block] = for {
    baseTarget <- arbitrary[Long]
    reference <- byteArrayGen(Block.BlockIdLength).map(r => ByteStr(r))
    generationSignature <- byteArrayGen(Block.GeneratorSignatureLength)
    assetBytes <- byteArrayGen(AssetIdLength)
    assetId = Some(ByteStr(assetBytes))
    sender <- accountGen
    recipient <- accountGen
    paymentTransaction: PaymentTransaction <- paymentGeneratorP(time, sender, recipient)
  } yield Block.buildAndSignV3(time, reference, NxtLikeConsensusBlockData(baseTarget, ByteStr(generationSignature)), Seq.fill(amt)(paymentTransaction), recipient, Set.empty).explicitGet()

  property(" block with txs bytes/parse roundtrip version 1,2") {
    Seq[Byte](1, 2).foreach { version =>
      forAll(blockGen) {
        case (baseTarget, reference, generationSignature, recipient, transactionData) =>
          val block = Block.buildAndSign(version, time, reference, NxtLikeConsensusBlockData(baseTarget, generationSignature), transactionData, recipient).explicitGet()
          val parsedBlock = Block.parseBytes(block.bytes).get
          assert(Signed.validateSignatures(block).isRight)
          assert(Signed.validateSignatures(parsedBlock).isRight)
          assert(parsedBlock.consensusData.generationSignature == generationSignature)
          assert(parsedBlock.version.toInt == version)
          assert(parsedBlock.signerData.generator.publicKey.sameElements(recipient.publicKey))
      }
    }
  }

  property(s" feature flags limit is ${Block.MaxFeaturesInBlock}") {
    val supportedFeatures = (0 to Block.MaxFeaturesInBlock * 2).map(_.toShort).toSet

    forAll(blockGen) {
      case (baseTarget, reference, generationSignature, recipient, transactionData) =>
        Block.buildAndSignV3(time, reference,
          NxtLikeConsensusBlockData(baseTarget, generationSignature), transactionData, recipient,
          supportedFeatures) should produce(s"Block could not contain more than ${Block.MaxFeaturesInBlock} feature votes")
    }
  }
  property(" block with txs bytes/parse roundtrip version 3") {
    val faetureSetGen : Gen[Set[Short]] = Gen.choose(0, Block.MaxFeaturesInBlock).flatMap(fc => Gen.listOfN(fc, arbitrary[Short])).map(_.toSet)

    forAll(blockGen, faetureSetGen) {
      case ((baseTarget, reference, generationSignature, recipient, transactionData), featureVotes) =>
        val block = Block.buildAndSignV3(time, reference, NxtLikeConsensusBlockData(baseTarget, generationSignature), transactionData, recipient, featureVotes).explicitGet()
        val parsedBlock = Block.parseBytes(block.bytes).get
        assert(Signed.validateSignatures(block).isRight)
        assert(Signed.validateSignatures(parsedBlock).isRight)
        assert(parsedBlock.consensusData.generationSignature == generationSignature)
        assert(parsedBlock.version.toInt == 3)
        assert(parsedBlock.signerData.generator.publicKey.sameElements(recipient.publicKey))
        assert(parsedBlock.featureVotes == featureVotes)
    }
  }

  ignore("sign time for 60k txs") {
    forAll(randomTransactionsGen(60000), accountGen, byteArrayGen(Block.BlockIdLength), byteArrayGen(Block.GeneratorSignatureLength)) { case ((txs, acc, ref, gs)) =>
      val (block, t0) = Instrumented.withTime(Block.buildAndSignV3(1, ByteStr(ref), NxtLikeConsensusBlockData(1, ByteStr(gs)), txs, acc, Set.empty).explicitGet())
      val (bytes, t1) = Instrumented.withTime(block.bytesWithoutSignature)
      val (hash, t2) = Instrumented.withTime(FastCryptographicHash.hash(bytes))
      val (sig, t3) = Instrumented.withTime(EllipticCurveImpl.sign(acc, hash))
      println((t0, t1, t2, t3))
    }
  }

  ignore("serialize and deserialize big block") {
    forAll(bigBlockGen(100 * 1000)) { case block =>
      val parsedBlock = Block.parseBytes(block.bytes).get
      Signed.validateSignatures(block) shouldBe 'right
      Signed.validateSignatures(parsedBlock) shouldBe 'right
    }
  }
}