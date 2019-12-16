/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Value;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JmxReporterTest {

    @Test
    public void testJmxRegistration() throws Exception {
        Metrics metrics = new Metrics();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            JmxReporter reporter = new JmxReporter();
            metrics.addReporter(reporter);

            assertFalse(server.isRegistered(new ObjectName(":type=grp1")));

            Sensor sensor = metrics.sensor("kafka.requests");
            sensor.add(metrics.metricName("pack.bean1.avg", "grp1"), new Avg());
            sensor.add(metrics.metricName("pack.bean2.total", "grp2"), new CumulativeSum());

            sensor.add(metrics.metricName("kebab-case-metric-name", "kebab-case-group"), new CumulativeSum());
            sensor.add(metrics.metricName("another-kebab-case-metricname", "PascalCaseKindOfGroup"), new CumulativeSum());
            sensor.add(metrics.metricName("PascalCaseMetricName", "another-kebab-case-groupname"), new Value());
            Map<String, String> tags = new HashMap<>();
            Map<String, String> pascalCaseTags = new HashMap<>();
            tags.put("tag1", "kebab-case-tag-name");
            pascalCaseTags.put("tag1", "KebabCaseTagName");
            tags.put("tag2", "PascalCaseTag");
            pascalCaseTags.put("tag2", "PascalCaseTag");
            sensor.add(metrics.metricName("NiceName", "not-nice-group", tags), new CumulativeSum());


            assertTrue(server.isRegistered(new ObjectName(":type=grp1")));
            assertEquals(Double.NaN, server.getAttribute(new ObjectName(":type=grp1"), "pack.bean1.avg"));
            assertTrue(server.isRegistered(new ObjectName(":type=grp2")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=grp2"), "pack.bean2.total"));

            MetricName metricName = metrics.metricName("pack.bean1.avg", "grp1");
            String mBeanName = JmxReporter.getMBeanName("", metricName);
            assertTrue(reporter.containsMbean(mBeanName));
            metrics.removeMetric(metricName);
            assertFalse(reporter.containsMbean(mBeanName));

            assertFalse(server.isRegistered(new ObjectName(":type=grp1")));
            assertTrue(server.isRegistered(new ObjectName(":type=grp2")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=grp2"), "pack.bean2.total"));

            assertTrue(server.isRegistered(new ObjectName(":type=kebab-case-group")));
            assertTrue(server.isRegistered(new ObjectName(":type=KebabCaseGroup")));
            assertTrue(server.isRegistered(new ObjectName(":type=another-kebab-case-groupname")));
            assertTrue(server.isRegistered(new ObjectName(":type=AnotherKebabCaseGroupname")));
            assertTrue(server.isRegistered(new ObjectName(":type=not-nice-group,tag1=kebab-case-tag-name,tag2=PascalCaseTag")));
            assertTrue(server.isRegistered(new ObjectName(":type=NotNiceGroup,tag1=KebabCaseTagName,tag2=PascalCaseTag")));

            assertEquals(server.getAttribute(new ObjectName(":type=kebab-case-group"), "kebab-case-metric-name"), server.getAttribute(new ObjectName(":type=KebabCaseGroup"), "KebabCaseMetricName"));
            assertEquals(server.getAttribute(new ObjectName(":type=PascalCaseKindOfGroup"), "another-kebab-case-metricname"), server.getAttribute(new ObjectName(":type=PascalCaseKindOfGroup"), "AnotherKebabCaseMetricname"));
            assertEquals(server.getAttribute(new ObjectName(":type=another-kebab-case-groupname"), "PascalCaseMetricName"), server.getAttribute(new ObjectName(":type=AnotherKebabCaseGroupname"), "PascalCaseMetricName"));

            MetricName kebabCaseMetricName = metrics.metricName("kebab-case-metric-name", "kebab-case-group");
            String kebabCaseMBeanName = JmxReporter.getMBeanName("", kebabCaseMetricName);
            MetricName kebabToPascalMetricName = metrics.metricName("KebabCaseMetricName", "KebabCaseGroup");
            String kebabToPascalMBeanName = JmxReporter.getMBeanName("", kebabToPascalMetricName);
            MetricName niceMetricName = metrics.metricName("NiceName", "not-nice-group", tags);
            String niceMBeanName = JmxReporter.getMBeanName("", niceMetricName);
            MetricName nicePascalCaseMetricName = metrics.metricName("NiceName", "NotNiceGroup", pascalCaseTags);
            String nicePascalCaseMBeanName = JmxReporter.getMBeanName("", nicePascalCaseMetricName);
            assertTrue(reporter.containsMbean(kebabCaseMBeanName));
            assertTrue(reporter.containsMbean(kebabToPascalMBeanName));
            assertTrue(reporter.containsMbean(niceMBeanName));
            metrics.removeMetric(kebabCaseMetricName);
            metrics.removeMetric(niceMetricName);
            assertFalse(reporter.containsMbean(mBeanName));
            assertFalse(reporter.containsMbean(kebabToPascalMBeanName));
            assertFalse(reporter.containsMbean(niceMBeanName));
            assertFalse(reporter.containsMbean(nicePascalCaseMBeanName));

            metricName = metrics.metricName("pack.bean2.total", "grp2");
            metrics.removeMetric(metricName);
            assertFalse(reporter.containsMbean(mBeanName));

            assertFalse(server.isRegistered(new ObjectName(":type=grp1")));
            assertFalse(server.isRegistered(new ObjectName(":type=grp2")));
            assertFalse(server.isRegistered(new ObjectName(":type=kebab-case-group")));
            assertFalse(server.isRegistered(new ObjectName(":type=KebabCaseGroup")));
            assertFalse(server.isRegistered(new ObjectName(":type=nice-group")));
        } finally {
            metrics.close();
        }
    }

    @Test
    public void testJmxRegistrationSanitization() throws Exception {
        Metrics metrics = new Metrics();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            metrics.addReporter(new JmxReporter());

            Sensor sensor = metrics.sensor("kafka.requests");
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo*"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo+"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo?"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo:"), new CumulativeSum());
            sensor.add(metrics.metricName("name", "group", "desc", "id", "foo%"), new CumulativeSum());

            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo\\*\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo\\*\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo+\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo+\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo\\?\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo\\?\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=\"foo:\"")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=\"foo:\""), "name"));
            assertTrue(server.isRegistered(new ObjectName(":type=group,id=foo%")));
            assertEquals(0.0, server.getAttribute(new ObjectName(":type=group,id=foo%"), "name"));

            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo*"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo+"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo?"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo:"));
            metrics.removeMetric(metrics.metricName("name", "group", "desc", "id", "foo%"));

            assertFalse(server.isRegistered(new ObjectName(":type=group,id=\"foo\\*\"")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=foo+")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=\"foo\\?\"")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=\"foo:\"")));
            assertFalse(server.isRegistered(new ObjectName(":type=group,id=foo%")));
        } finally {
            metrics.close();
        }
    }
}
