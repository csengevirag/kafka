package kafka.metrics

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup._
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

import scala.collection.JavaConverters._

class MetricNameTest {

  @Test
  def testNewGauge(): Unit = {
    newGauge("kebab-case-name",
      new Gauge[Int] {
        def value = 3
      })
    val tags: collection.mutable.Map[String, String] = collection.mutable.Map.empty
    tags.update("tag1", "kebab-case-tag")
    tags.update("tag2", "PascalCaseTag")

    newGauge("PascalCaseName",
      new Gauge[String] {
        def value = "value"
      }, tags)

    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=PascalCaseName,tag1=kebab-case-tag,tag2=PascalCaseTag") == 1)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=PascalCaseName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-name") == 1)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseName") == 1)

    removeMetric("PascalCaseName", tags)
    removeMetric("kebab-case-name")
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-name") == 1)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseName") == 1)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=PascalCaseName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-name") == 0)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=PascalCaseName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 0)
  }

  @Test
  def testNewMeter(): Unit = {
    val tags: collection.mutable.Map[String, String] = collection.mutable.Map.empty
    tags.update("tag1", "kebab-case-tag")
    tags.update("tag2", "PascalCaseTag")
    newMeter("kebab-case-meter-name", "view", TimeUnit.SECONDS, tags)

    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-meter-name,tag1=kebab-case-tag,tag2=PascalCaseTag") == 1)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseMeterName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)

    removeMetric("kebab-case-meter-name", tags)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-meter-name,tag1=kebab-case-tag,tag2=PascalCaseTag") == 1)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseMeterName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)

  }

  @Test
  def testHistogram(): Unit = {
    val tags: collection.mutable.Map[String, String] = collection.mutable.Map.empty
    tags.update("tag1", "kebab-case-tag")
    tags.update("tag2", "PascalCaseTag")
    newHistogram("kebab-case-histogram-name", true, tags)

    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-histogram-name,tag1=kebab-case-tag,tag2=PascalCaseTag") == 1)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseHistogramName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)

    removeMetric("kebab-case-histogram-name", tags)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-histogram-name,tag1=kebab-case-tag,tag2=PascalCaseTag") == 1)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseHistogramName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)

  }

  @Test
  def testTimer(): Unit = {
    val tags: collection.mutable.Map[String, String] = collection.mutable.Map.empty
    tags.update("tag1", "kebab-case-tag")
    tags.update("tag2", "PascalCaseTag")
    newHistogram("kebab-case-timer-name", true, tags)

    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-timer-name,tag1=kebab-case-tag,tag2=PascalCaseTag") == 1)
    assertTrue(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseTimerName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)

    removeMetric("kebab-case-timer-name", tags)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=kebab-case-timer-name,tag1=kebab-case-tag,tag2=PascalCaseTag") == 1)
    assertFalse(Metrics.defaultRegistry().allMetrics().keySet.asScala.count(_.getMBeanName == "kafka.metrics:type=KafkaMetricsGroup,name=KebabCaseTimerName,tag1=KebabCaseTag,tag2=PascalCaseTag") == 1)

  }
}
