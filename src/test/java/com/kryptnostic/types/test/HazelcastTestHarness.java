package com.kryptnostic.types.test;

import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;

public class HazelcastTestHarness {

	protected static HazelcastInstance hazelcast = null;

	@BeforeClass
	public static final void initHazelcast() {
		if (hazelcast == null) {
			Config config = new Config("test");
			config.setGroupConfig(new GroupConfig("test", "osterone"));
			config.setNetworkConfig(new NetworkConfig().setPort(5801).setPortAutoIncrement(true)
					.setJoin(new JoinConfig().setMulticastConfig(new MulticastConfig().setEnabled(false))));

			hazelcast = Hazelcast.newHazelcastInstance(config);
		}

	}

	@AfterClass
	public static final void shutdownHazelcast() {
		hazelcast.shutdown();
		hazelcast = null;
	}

	@Test
	public void simpleTest() {
		IMap<String, String> test = hazelcast.getMap("test");

		test.put("world", "hello");
		test.put("hello", "world");
		test.put("bye", "sam");

		test.executeOnEntries(new AbstractEntryProcessor<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object process(Entry<String, String> entry) {
				System.out.println(entry.getValue());
				return null;
			}

		});
	}
}
