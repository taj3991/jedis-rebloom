package redis.rebloom.bloomfilter;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @since 2018/10/31 20:14
 */
public class BFClusterClient extends JedisCluster {

	public BFClusterClient(Set<HostAndPort> nodes) {
		super(nodes);
	}

	public BFClusterClient(Set<HostAndPort> nodes, int timeout) {
		super(nodes, timeout);
	}

	public BFClusterClient(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
		super(nodes, timeout, maxAttempts);
	}

	public BFClusterClient(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
		super(nodes, poolConfig);
	}

	public BFClusterClient(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
		super(nodes, timeout, poolConfig);
	}

	public BFClusterClient(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, timeout, maxAttempts, poolConfig);
	}

	public BFClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
	}

	public BFClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
	}

	public BFClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
		super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
	}


	private Connection sendCommand(Jedis jedis, ProtocolCommand command, String... args) {
		jedis.getClient().sendCommand(command, args);
		return jedis.getClient();
	}


	private Connection sendCommand(Jedis jedis, ProtocolCommand command, byte[]... args) {
		jedis.getClient().sendCommand(command, args);
		return jedis.getClient();
	}


	/**
	 * Adds an item to the Bloom Filter, creating the filter
	 * with defaultErrorRate = 0.01 and defaultInitCapacity=100
	 * if it does not yet exist.
	 *
	 * @param key  The name of the filter
	 * @param item The value to add to the filter
	 * @return true if the item was not previously in the filter.
	 */
	public boolean add(final String key, final String item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, BFCommand.ADD, key, item).getIntegerReply() != 0;
			}
		}.run(key);
	}

	/**
	 * Adds an item to the Bloom Filter, creating the filter
	 * with defaultErrorRate = 0.01 and defaultInitCapacity=100
	 * if it does not yet exist.
	 * Similar to {@link #add(String, String)}
	 *
	 * @param key  The name of the filter
	 * @param item The value to add to the filter
	 * @return true if the item was not previously in the filter.
	 */
	public boolean add(final String key, final byte[] item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, BFCommand.ADD, key.getBytes(), item).getIntegerReply() != 0;
			}
		}.run(key);
	}


	/**
	 * Adds one or more items to the Bloom Filter, creating the filter
	 * with defaultErrorRate = 0.01 and defaultInitCapacity=100
	 * if it does not yet exist.
	 * This command operates identically to BF.ADD except it allows multiple inputs and returns multiple values.
	 *
	 * @param key   The name of the filter
	 * @param items One or more items to add
	 * @return An array of booleans (integers). Each element is either true or false depending on whether
	 * the corresponding input element was newly added to the filter or may have previously existed.
	 */
	public Boolean[] addMulti(final String key, final String... items) {
		return new JedisClusterCommand<Boolean[]>(connectionHandler, maxAttempts) {
			@Override
			public Boolean[] execute(Jedis jedis) {
				return sendMultiCommand(jedis, BFCommand.MADD, key, items);
			}
		}.run(key);
	}


	/**
	 * Adds one or more items to the Bloom Filter, creating the filter
	 * with defaultErrorRate = 0.01 and defaultInitCapacity=100
	 * if it does not yet exist.
	 * This command operates identically to BF.ADD except it allows multiple inputs and returns multiple values.
	 * Similar to {@link #addMulti(String, String...)}
	 *
	 * @param key   The name of the filter
	 * @param items One or more items to add
	 * @return An array of booleans (integers). Each element is either true or false depending on whether
	 * the corresponding input element was newly added to the filter or may have previously existed.
	 */
	public Boolean[] addMulti(final String key, final byte[]... items) {
		return new JedisClusterCommand<Boolean[]>(connectionHandler, maxAttempts) {
			@Override
			public Boolean[] execute(Jedis jedis) {
				return sendMultiCommand(jedis, BFCommand.MADD, key.getBytes(), items);
			}
		}.run(key);
	}

	@SafeVarargs
	private final <T> Boolean[] sendMultiCommand(Jedis jedis, ProtocolCommand cmd, T key, T... items) {
		ArrayList<T> arr = new ArrayList<>();
		arr.add(key);
		arr.addAll(Arrays.asList(items));
		List<Long> reps;
		if (key instanceof String) {
			reps = sendCommand(jedis, cmd, (String[]) arr.toArray((String[]) items)).getIntegerMultiBulkReply();
		} else {
			reps = sendCommand(jedis, cmd, (byte[][]) arr.toArray((byte[][]) items)).getIntegerMultiBulkReply();
		}
		Boolean ret[] = new Boolean[items.length];
		for (int i = 0; i < reps.size(); i++) {
			ret[i] = reps.get(i) != 0;
		}
		return ret;

	}


	/**
	 * Creates an empty Bloom Filter with a given desired error ratio and initial capacity.
	 * This command is useful if you intend to add many items to a Bloom Filter,
	 * otherwise you can just use BF.ADD to add items.
	 * It will also create a Bloom Filter for you if one doesn't already exist.
	 * The initial size and error rate will dictate the performance and memory usage of the filter.
	 * In general, the smaller the error rate (i.e. the lower the tolerance for false positives)
	 * the greater the space consumption per filter entry.
	 *
	 * @param key          The key under which the filter is to be found
	 * @param errorRate   The desired probability for false positives. This should be a decimal value between 0 and 1.
	 *                     For example, for a desired false positive rate of 0.1% (1 in 1000), error_rate should be set to 0.001.
	 *                     The closer this number is to zero,the greater the memory consumption per item and the more CPU usage per operation
	 * @param initCapacity The number of entries you intend to add to the filter.
	 *                     Performance will begin to degrade after adding more items than this number.
	 *                     The actual degradation will depend on how far the limit has been exceeded.
	 *                     Performance will degrade linearly as the number of entries grow exponentially.
	 * @return
	 */
	public boolean createFilter(String key, double errorRate, long initCapacity) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				String[] args = new String[3];
				args[0]= key;
				args[1]= errorRate+"";
				args[2]= initCapacity + "";
				return sendCommand(jedis, BFCommand.RESERVE, args).getStatusCodeReply() != "OK";
			}
		}.run(key);
	}


	/**
	 * Determines whether an item may exist in the Bloom Filter or not.
	 *
	 * @param key  The name of the filter
	 * @param item The item to check for
	 * @return false if the item certainlty does not exist, true if the item may exist.
	 * Because this is a probablistic data structure, false positives (but not false negatives) may be returned.
	 */
	public boolean exists(String key, String item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, BFCommand.EXISTS, key, item).getIntegerReply() != 0;
			}
		}.run(key);
	}


	/**
	 * Determines whether an item may exist in the Bloom Filter or not..
	 * Similar to {@link #exists(String, String)}
	 *
	 * @param key  The name of the filter
	 * @param item The item to check for
	 * @return false if the item certainlty does not exist, true if the item may exist.
	 * Because this is a probablistic data structure, false positives (but not false negatives) may be returned.
	 */
	public boolean exists(String key, byte[] item) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, BFCommand.EXISTS, key.getBytes(), item).getIntegerReply() != 0;
			}
		}.run(key);
	}

	/**
	 * Determines whether more items may exist in the Bloom Filter or not.
	 *
	 * @param key  The name of the filter
	 * @param item The item to check for
	 * @return false if the item certainlty does not exist, true if the item may exist.
	 * Because this is a probablistic data structure, false positives (but not false negatives) may be returned.
	 */
	public Boolean[] existsMulti(String key, String... item) {
		return new JedisClusterCommand<Boolean[]>(connectionHandler, maxAttempts) {
			@Override
			public Boolean[] execute(Jedis jedis) {
				return sendMultiCommand(jedis, BFCommand.MEXISTS, key, item);
			}
		}.run(key);
	}


	/**
	 * Determines whether more items may exist in the Bloom Filter or not.
	 * Similar to {@link #existsMulti(String, String...)}
	 *
	 * @param key  The name of the filter
	 * @param items The items to check for
	 * @return false if the item certainlty does not exist, true if the item may exist.
	 * Because this is a probablistic data structure, false positives (but not false negatives) may be returned.
	 */
	public Boolean[] existsMulti(String key, byte[]... items) {
		return new JedisClusterCommand<Boolean[]>(connectionHandler, maxAttempts) {
			@Override
			public Boolean[] execute(Jedis jedis) {
				return sendMultiCommand(jedis, BFCommand.MEXISTS, key.getBytes(), items);
			}
		}.run(key);
	}

	/**
	 * Delte the filter
	 *
	 * @param key The key under which the filter is to be found
	 */
	public boolean deleteFilter(String key) {
		return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
			@Override
			public Boolean execute(Jedis jedis) {
				return sendCommand(jedis, BFCommand.DEL, key).getIntegerReply() != 0;
			}
		}.run(key);
	}

}
