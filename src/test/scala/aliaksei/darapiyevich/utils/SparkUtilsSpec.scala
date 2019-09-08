package aliaksei.darapiyevich.utils

import java.math.BigDecimal

import aliaksei.darapiyevich.UnitTestSpec

class SparkUtilsSpec extends UnitTestSpec {

  "mean()" should "return value at the middle when number of values is odd" in {
    val values = Seq(1, 2, 3, 4, 5).map(new BigDecimal(_))
    val result = SparkUtils.median(values)
    result shouldBe 3
  }

  it should "return mean of 2 elements in the middle when number of values is even" in {
    val values = Seq(1, 2, 3, 4).map(new BigDecimal(_))
    val result = SparkUtils.median(values)
    result shouldBe 2.5
  }

}
