package kafka.server

import java.util.jar.JarFile

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import collection.JavaConverters._


class VersionInfo extends Logging with KafkaMetricsGroup {
  def getBrokerVersion(): String = {
    val list = Thread.currentThread().getContextClassLoader.getResources(JarFile.MANIFEST_NAME).asScala
    val urls = list.map(url => Option(url.openStream())).find().get.get
    val manifest = new java.util.jar.Manifest(urls)
    val version = Option(manifest.getMainAttributes().getValue("Version"))
    if (version.isDefined) {
      println(version.get)
      version.get
    }
    "unknown version"
  }
  newGauge("BrokerVersion",
    new Gauge[String] {
      def value: String = getBrokerVersion()
    }
  )

}
