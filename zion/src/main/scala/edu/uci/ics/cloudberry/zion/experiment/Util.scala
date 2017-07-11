package edu.uci.ics.cloudberry.zion.experiment

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoint, WeightedObservedPoints}
import org.apache.commons.math3.special.Erf

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jianfeng on 4/16/17.
  */
class OtherTasks {

}

object Stats extends App {

  import scala.collection.JavaConverters._

  /**
    * a0 + a1 * x
    */
  case class Coeff(a0: Double, a1: Double) {
    override def toString: String = s"a1=$a1, a0=$a0"
  }

  case class Coeff3(a2: Double, a1: Double, a0: Double) {
    override def toString: String =
      s"""
         |a2=$a2
         |a1=$a1
         |a0=$a0
       """.stripMargin
  }

  def distance(xa: Double, xb: Double, x: Double, y: Double): Double = {
    val thY = xb * x + xa
    val thX = (y - xa) / xb
    val lineA = Math.abs(x - thX)
    val lineB = Math.abs(y - thY)
    val lineC = Math.sqrt(lineA * lineA + lineB * lineB)
    lineA * lineB / lineC
  }

  def linearFitting(obs: WeightedObservedPoints): Coeff = {
    val filter: PolynomialCurveFitter = PolynomialCurveFitter.create(1)
    val ret = filter.fit(obs.toList)
    Coeff(ret(0), ret(1))
  }

  def calcVariance(obs: WeightedObservedPoints, coeff: Coeff): Double = {
    val list = obs.toList.asScala
    var variance = 0.0
    for (ob: WeightedObservedPoint <- list) {
      val y = coeff.a0 + coeff.a1 * ob.getX
      variance += (y - ob.getY) * (y - ob.getY)
    }
    variance / list.size
  }

  def calcLinearCoeff(x1: Double, y1: Double, x2: Double, y2: Double): Coeff = {
    val a1 = (y2 - y1) / (x2 - x1)
    val a0 = y1 - a1 * x1
    Coeff(a0, a1)
  }

  def calcThreeAppxLine(variance: Double): Seq[Coeff] = {
    val stdDev = Math.sqrt(variance)
    val norm = new NormalDistribution(null, 0, stdDev)
    println(norm.density(0), norm.density(stdDev), norm.density(stdDev * 2), norm.density(stdDev * 3))
    val coeff1 = calcLinearCoeff(0, norm.density(0), stdDev, norm.density(stdDev))
    val coeff2 = calcLinearCoeff(stdDev, norm.density(stdDev), 2 * stdDev, norm.density(2 * stdDev))
    val coeff3 = calcLinearCoeff(stdDev * 2, norm.density(stdDev * 2), 3 * stdDev, norm.density(3 * stdDev))
    Seq(coeff1, coeff2, coeff3)
  }

  def calcBeta(stdDev: Double): Double = {
    val norm = new NormalDistribution(null, 0, stdDev)
    calcLinearCoeff(0, norm.density(0), stdDev * 3, 0).a1
  }

  def withRxAlpha(alphaRaw: Double, a0: Double, W: Double, ceffs: Seq[Double]): Seq[Double] = {
    // rx/Rw - alpha / Wi |ceffs|
    val alphaW = alphaRaw / W
    val c3 = -alphaW * ceffs(0)
    val c2 = -alphaW * ceffs(1)
    val c1 = 1 / (W - a0) - alphaW * ceffs(2)
    val c0 = -alphaW * ceffs(3) - a0 / (W - a0)
    Seq(c3, c2, c1, c0)
  }

  def deriviation(a: Double, b: Double, c: Double): (Double, Double) = {
    ((-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a),
      (-b - Math.sqrt(b * b - 4 * a * c)) / (2 * a))
  }


  def mulAlpha(alpha: Double, p: Double, a2: Double, a1: Double, a0: Double): Coeff3 = {
    Coeff3(-p * alpha * a2, 1 - p * alpha * a1, -p * alpha * a0)
  }

  def getOptimalRx(range: Double, W: Double, stdDev: Double, alpha: Double, a0: Double, a1: Double): Double = {
    val R = range
    val Rw = (W - a0) / a1
    val optimalValueZ = 2 * W / (a1 * R * alpha) - 1
    if (optimalValueZ < -1) {
      0
    } else if (optimalValueZ > 0) {
      Rw
    } else {
      val z = Erf.erfInv(optimalValueZ)
      Math.min(Rw, Math.max(0, (Math.sqrt(2) * stdDev * z + W - a0) / a1))
    }
  }

  def useRealGaussian(W: Double, stdDev: Double, alpha: Double, a0: Double, a1: Double): Unit = {

    def format(n: Double): String = "%.5f".format(n)

    val R = 100.0
    val Rw = (W - a0) / a1
    val strSigma = f"$stdDev%1.2f"
    val strAlpha = f"${alpha}%1.5f"
    val optimalRx = getOptimalRx(R, W, stdDev, alpha, a0, a1)

    val optimizedZ = (a1 * optimalRx + a0 - W) / (Math.sqrt(2) * stdDev)
    val optimizedProgress = optimalRx / R - alpha * stdDev / (W * Math.sqrt(2)) * (1 / Math.sqrt(Math.PI) * Math.exp(-(optimizedZ * optimizedZ)) + optimizedZ * (1 + Erf.erf(optimizedZ)))

    println(s"optimal rx: $optimalRx, z: $optimizedZ, progress: $optimizedProgress")

    val strA = s" (${format(a0)} + $a1 *x - $W)/ ${format(Math.sqrt(2) * stdDev)}"
    println(
      s"""
         |x/$R - ${format(alpha * stdDev / (Math.sqrt(2) * W))} * ( ${format(1 / Math.sqrt(Math.PI))}* e^(-($strA)^2) + ($strA)*(1+ erf($strA)))
         |
         |set key left box
         |set autoscale
         |set samples 800
         |set terminal postscript eps enhanced size 3in,3in
         |
         |x2(x) = x*x
         |a(W, o, a1, a0,x) = (a1*x + a0 - W) / (sqrt(2)*o)
         |E(R, alpha, W, o, a1, a0, x) = x/R - alpha * o/(W * sqrt(2)) * ( 1/sqrt(3.14)* exp(-x2(a(W,o,a1,a0,x))) + a(W,o,a1,a0,x) * (1 + erf(a(W,o,a1,a0,x))))
         |
         |set arrow from $optimalRx,-1 to $optimalRx,1 nohead;
         |plot [0:$Rw] E($R, $alpha,$W, $stdDev, $a1, $a0, x) \\
         |      ti '{/Symbol a}=$strAlpha, {/Symbol o}=$strSigma',
       """.stripMargin)


  }

  def useHistogram(l: Double, alpha: Double, a0: Double, a1: Double): Unit = {
    val R = 100.0
    val Pr = Seq(0.01, 0.03, 0.12, 0.34, 0.26, 0.22, 0.02)
    val b = 0.1
    val j = 3

    val string =
      s"""
         |set key left box
         |set autoscale
         |set samples 800
         |set terminal postscript eps enhanced size 3in,3in
         |
         |g(x)=$a1*x+$a0
         |f(k, g) = l - (k - $j)*$b
         |y3(x) = x*x*x
         |E(k, R,l,  alpha, a0, a1, pk, sumy, const, x) = (l - x - a0 - (k-$j)*$b) / (a1*R) - alpha/l * ( \\
         |       pk/(2* $b*$b) * y3($b-x) + \\
         |       -sumy*x + const)
         |
       """.stripMargin

    println(string)
    j until Pr.size foreach { k =>
      val pk = Pr(k)
      val sumy = Pr.slice(k + 1, Pr.size).sum
      val const = Pr.zipWithIndex.slice(k + 1, Pr.size).map {
        case (p, i) => b * p * (0.5 + i - k)
      }.sum

      val gl = l - (k - j + 1) * b
      val gh = l - (k  -j)*b
      val plot = s"plot [0:0.1] E($k, $R,$l, $alpha, $a0, $a1, $pk, $sumy,$const,x)"
      println(plot)
    }
  }

  def useNormalizedLinearFunction(C: Double, stdDev: Double, alpha: Double): Unit = {
    val norm = new NormalDistribution(0, stdDev)
    val b = norm.density(0)
    val b2 = b * b
    val c2 = C * C
    val c3 = C * c2
    val c0 = -alpha * (-b2 / 6 * c3 + b / 2 * c2 - C / 2 + 1 / (6 * b))
    val x0 = 1 / b
    println(s"x0 = $x0, c0 = $c0, stdDev:$stdDev")
    println(
      s"""
         |
         |set key left box
         |set autoscale
         |set samples 800
         |set terminal postscript eps enhanced size 3in,3in
         |plot [${C - x0}:$C] x+$c0
       """.stripMargin)
  }

  class Histogram(step: Double) {
    require(step > 0)
    private var sum: Double = 0.0
    private val posCounts = new ArrayBuffer[Int]
    private val negCounts = new ArrayBuffer[Int]

    def +=(value: Double): Unit = {
      val id = (value / step).toInt
      val count = if (value >= 0) posCounts else negCounts
      val absId = if (value >= 0) id else -id
      count.size to absId foreach (_ => count += 0)
      count(absId) += 1
      sum += 1
    }

    def prob(id: Int): Double = {
      val (rid, count) = if (id >= 0) (id, posCounts) else (-id, negCounts)
      if (rid < count.size) {
        count(rid) / sum
      } else {
        0.0
      }
    }

    /**
      * cumunitative probs >= id
      *
      * @param id
      * @return
      */
    def cumProb(id: Int): Double = {
      val (rid, count) = if (id >= 0) (id, posCounts) else (-id, negCounts)
      if (rid < count.size) {
        val s = count.slice(rid, count.size).sum
        s / sum
      } else {
        0
      }
    }
  }

  def useHistogramUniformFunction(range: Double, W: Double, b: Double, a0: Double, a1: Double, alpha: Double, probs: Seq[Double]): Seq[Double] = {
    val b2 = b * b
    val seqIntegral: Seq[Double] = 1 to probs.size map (i => 0.5 * b2 + (i - 1) * b2)
    //    println(seqIntegral)

    def value(j: Int) = probs.slice(j, probs.size).zip(seqIntegral).map { case (p: Double, v: Double) => p * v }.sum

    //        0 to (probs.size - 1) foreach (j => println(s"gain:${(W - j * b - a0) / (a1 * range)}, penalty:alpha * ${value(j)/W} = ${alpha / W * value(j)}"))

    0 to (probs.size - 1) map (j => (W - j * b - a0) / (a1 * range) - (alpha / b / W) * value(j))
  }

  val obs: WeightedObservedPoints = new WeightedObservedPoints()
  Seq((1, 0.5), (7, 3.9), (2, 1.8)).foreach(x => obs.add(x._1, x._2))
  //  val coeff = linearFitting(obs)
  val coeff = Coeff(0.33548, 0.51935)
  //  println(coeff)
  val variance = calcVariance(obs, coeff)
  //    val variance = 0.25
  //  val stdDev = Math.sqrt(variance)
  val stdDev = 1
//  println(variance, stdDev)

  val C = 2.2
  Seq(1).foreach { alpha =>
    //  useNormalizedLinearFunction(C, stdDev, alpha)
    //  useOneLinearFunction(C, stdDev, alpha, coeff.a0, coeff.a1)
//    useRealGaussian(C, stdDev, alpha, coeff.a0, coeff.a1)
    useHistogram(C, 1, 0.55, 1.0)
    //    val px = useHistogramUniformFunction(100, 2, 0.2, 0.3, 0.5, alpha, Seq(0.35, 0.26, 0.24, 0.01))
    //    println(px)
  }
}

object Data {
  val ids = Seq(682833636383522817l, 683196024362524672l, 683558412152487936l, 683920800517238784l, 684283187854503936l, 684645575917191168l, 685007964231450626l, 685370351824441344l, 685732739362983936l, 686095127274745856l, 686457515417202689l, 686819903249096704l, 687182290766544896l, 687544679957565440l, 687907067114450944l, 688269454833135616l, 688631842329595904l, 688994230262366208l, 689356618866245632l, 689719005855100928l, 690081393783758848l, 690443781666140161l, 690806169850351616l, 691168557384798208l, 691530945178898432l, 691893335703814144l, 692255721619066880l, 692618109736218624l, 692980497312514048l, 693342885450760192l, 693705272582348800l, 694067660364062720l, 694430049261506565l, 694792436619595776l, 695154823843319808l, 695517211721654272l, 695879600660819968l, 696241987389751296l, 696604375859359744l, 696966763213246464l, 697329151716433920l, 697691538931867648l, 698053927367999488l, 698416315061514244l, 698778702654603264l, 699141090457194497l, 699503478377312256l, 699865866180042752l, 699965866180042752l, 699985866180042752l, 699995866180042752l, 700088225913208833l, 701888225913208833l, 702040193264541696l, 702402581679742976l, 702764969058820096l, 703127357566033920l, 703489744789962752l, 703852132974211076l, 704214520797868033l, 704576909309444096l, 704939296193699840l, 705301684151558144l, 705664072113455104l, 706026460461436928l, 706388848801161216l, 706751235710300160l, 707113623680737281l, 707476011516960768l, 707838399105912832l, 708200787415937024l, 708563174950211584l, 708925563100987393l, 709272851065524224l, 709635239069351936l, 709997626947719168l, 710360015190646785l, 710460015190646785l, 710560015190646785l, 710660015190646785l, 710760015190646785l, 710860015190646785l, 710960015190646785l, 711060015190646785l, 711160015190646785l, 711260015190646785l, 711360015190646785l, 711460015190646785l, 711560015190646785l, 711660015190646785l, 715635459397967872l, 715795833350959104l, 716158221178634241l, 716520608746450944l, 716882996301705218l, 717245385140342784l, 717607772418883584l, 717970159994929153l, 718332548129157120l, 718694935755489280l, 719057323486875648l, 719419711373463552l, 719782099222310912l, 720144487243120640l, 720506875096276992l, 720869263913783296l, 721231651112488961l, 721594038634164224l, 721956427716124673l, 722318814898065408l, 722681202109521920l, 723043589991751680l, 723405978645893121l, 723768365982994432l, 724130753819324416l, 724493141533925378l, 724855529445662720l, 725217917277626368l, 725580305139118080l, 725942693415636992l, 726305080828293120l, 726667468861771776l, 727029856836444161l, 727392244345397252l, 727754632425054209l, 728117020256964608l, 728479407954763777l, 728841796021682176l, 729204183736291328l, 729566572063248385l, 729928959509471232l, 730291347458916352l, 730653735232266241l, 731016123488014336l, 731378511475281920l, 731740898745155584l, 732103286627520512l, 732465674694451200l, 732828062736338944l, 733190450325032962l, 733552838144450560l, 733915225921970180l, 734277613884190720l, 734640001921757184l, 735002389715910656l, 735364777799733248l, 735727165497442304l, 736089553325281281l, 736451941061042176l, 736814328964218880l, 737176716871761920l, 737539104741556224l, 737901492447907840l, 738263880514736128l, 738626268300607488l, 738988656141049858l, 739351043973251072l, 739713432023269377l, 740075819754655744l, 740438207511269378l, 740800595376939009l, 741162983603179520l, 741525371107966976l, 741887759330082816l, 742250146860146688l, 742612534788653056l, 742974922692173824l, 743337310708604930l, 743699698343510016l, 744062086490300417l, 744424474104184832l, 744786861898399744l, 745149249814290432l, 745511637637947392l, 745874025872769024l, 746236413427912705l, 746598801427693569l, 746961189175861248l, 747323577003761673l, 747685964923932672l, 748048352693133313l, 748595703279136768l, 748773128977866753l, 749135516306509828l, 749497904352653312l, 749860292188790784l, 750222680440180736l, 750585068138205185l, 750947455764758528l, 751309843613356032l, 751672231437164544l, 752034619344564224l, 752397007134658563l, 752759394950012928l, 753121782895370240l, 753484170744041473l, 753846558941073408l, 754208946458681344l, 754571334613512192l, 754933722260967425l, 755296110143471616l, 755658497920995328l, 756020885916491777l, 756383274432204801l, 756745661605941248l, 757108049576206336l, 757470437446082560l, 757832825168990208l, 758195212988588032l, 758557600942264320l, 758919988891660288l, 759282376694521856l, 759644764383813633l, 760007152450740224l, 760369540186288128l, 760731928911904772l, 761094315921645568l, 761456704223444992l, 761819091736723456l, 762181479652745216l, 762543867430264832l, 762906255341998080l, 763268643295596545l, 763631031115128832l, 763993419064549376l, 764355806909370368l, 764928744683245568l, 765080582510358529l, 765442970560630784l, 765805358292033536l, 766167746220482560l, 766530133834338304l, 766892522090160128l, 767441549806346240l, 767617298324721664l, 767979685741531136l, 768342073271394304l, 768704461032321024l, 769066850294562816l, 769429236901961728l, 769929037821075456l, 770154012637265920l, 770516400485961728l, 770878788838326272l, 771241176775036932l, 771603563986300928l, 771965952099491840l, 772440663098990592l, 772690727960653824l, 773053115603877888l, 773415503352229888l, 773938687327477761l, 774140279364341760l, 774502667099910145l, 774865055083036673l, 775227442722054144l, 775609198147096576l, 775952218348400640l, 776314606981619712l, 776676994721406976l, 777039382121439232l, 777401770033020929l, 777764158213271552l, 778126546175455232l, 778488933961400320l, 778851321755471872l, 779213709935775744l, 779576097461506048l, 779938485281103872l, 780300872785862657l, 780663260768940032l, 781025648634716160l, 781388036516880385l, 781750424504143872l, 782112812558610432l, 782475200034181121l, 782837587790659584l, 783199976201465856l, 783562363890839552l, 783924752922451968l, 784287139735359492l, 784649528179777537l, 785011916255088641l, 785374302971408384l, 785736691072073728l, 786099079105376256l, 786461467310694401l, 786823854903525380l, 787186242211282944l, 787548630064390144l, 787911018836099072l, 788273405954920448l, 788635793917112320l, 788998182080393216l, 789360569728065536l, 789460569728065536l, 789560569728065536l, 789660569728065536l, 791046721530920961l, 791172508762255361l, 791534897496072192l, 791897284615122944l, 792259672388632580l, 792622060157542401l, 792984448656547840l, 793346835893157888l, 793709223754420225l, 794071612001808384l, 794433999791915008l, 794796387351207936l, 795158776722505728l, 795536262669053952l, 795898650560000000l, 796261038324711424l, 796623426668498945l, 796985814534197248l, 797348202081095680l, 797710589971853312l, 798072978315456512l, 798435365698883584l, 798797753518170114l, 799160141752991748l, 799522529132085248l, 799884917291229184l, 800247305093967873l, 800609693215297536l, 800972080971857920l, 801334468577525762l, 801696856296148992l, 802059244229033984l, 802421632254046208l, 802784020132196352l, 803146408173785089l, 803508795632553984l, 804207381714497536l, 804233572097949696l, 804595959250481152l, 804958347334131712l, 805320735170248704l, 805683122855669760l, 806045510880686082l, 806407899174105088l, 807132674573893632l, 807495062535991296l, 807857450741329920l, 808219838053126144l, 808582226006970368l, 808944614124163072l, 809307001700237312l, 809669390425735168l, 810031777410486272l, 810394165053771776l, 810756553011699712l, 811118941032644608l, 811481328743055360l, 811843717216800768l, 812206104482430976l, 812568492440489985l, 812930880289247232l, 813293268276510720l, 813655656859193344l, 814018043886104576l, 814380431864799232l, 814742819508199424l, 815105207428259845l)
  val P1: Double = 0.341
  val P2: Double = 0.136
  val P3: Double = 0.021
}

object TestDouble extends App {
  val p = Data.P1

  val obs: WeightedObservedPoints = new WeightedObservedPoints()

  // Example distribution
  Seq[(Double, Double)](
    (7.8, 5.071),
    (3.2, 2.302),
    (2.6, 2.305),
    (1.6, 2.151),
    (0.8, 1.331),
    (1, 1.431),
    (1.6, 1.842),
    (2.2, 2.764),
    (1.4, 1.149),
    (3.4, 2.809),
    (2.4, 2.183),
    (2, 2.457),
    (1, 1.536),
    (1.2, 1.227),
    (2.6, 3.071),
    (1.2, 1.535),
    (1.6, 1.193),
    (3.8, 3.154),
    (2, 1.796),
    (2.2, 2.186),
    (1.8, 1.498),
    (2.6, 2.764),
    (1.4, 1.284),
    (2.4, 2.709),
    (1.2, 1.394),
    (1.8, 1.676),
    (2.6, 3.072),
    (1, 1.546),
    (1, 1.217),
    (2, 2.575),
    (1.4, 2.564),
    (0.8, 1.824),
    (0.6, 1.939)
  ) //.foreach( x=>obs.add(x._1.toDouble, x._2.toDouble))

  Seq[(Double, Double)](
    (23.2, 3.615),
    (39, 6.773),
    (6.4, 0.919),
    (16, 2.072),
    (17.6, 2.241),
    (16.4, 1.521),
    (28.2, 4.913),
    (0.2, 0.082),
    (4.2, 1.182),
    (9.6, 2.538),
    (8.4, 2.018),
    (9.4, 2.681),
    (5.8, 2.031),
    (4.6, 0.953),
    (13.4, 2.328),
    (14.2, 2.457),
    (12, 1.767),
    (16, 2.909),
    (8, 1.835),
    (7, 1.465),
    (10.4, 2.828),
    (5, 1.162),
    (9.6, 2.563),
    (6.2, 1.739),
    (7, 2.101),
    (6.2, 2.3),
    (4.2, 1.496),
    (6, 3.007),
    (2, 0.917),
    (4.8, 2.25),
    (4.2, 1.947),
    (4.4, 2.662),
    (2.2, 2.76)
  ).foreach(x => obs.add(x._1.toDouble, x._2.toDouble))

  //  val coeff = Stats.linearFitting(obs)
  val filter: PolynomialCurveFitter = PolynomialCurveFitter.create(1)
  val ret = filter.fit(obs.toList)
  //    Coeff(ret(0), ret(1))
  println(ret.toList)
}

object TestHistorgram extends App {
  val b = 0.5
  val alpha = 0.1
  val W = 2
  val a0 = 0.2
  val histo = new Stats.Histogram(b)
  Seq(-1792, -907, -1040, 383, 290, 414, 1098, 637, 3000).foreach(d => histo += (d / 1000.0))
  val n = (W / b).toInt
  val probs = ((0 to (n - 1)).map(histo.prob) ++ Seq(histo.cumProb(n)))
  println(probs)

  //  val exp = Stats.useHistogramUniformFunction(W, b, a0, alpha, probs)
  //  println(exp)
}
