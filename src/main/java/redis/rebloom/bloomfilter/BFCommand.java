package redis.rebloom.bloomfilter;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;


public enum BFCommand implements ProtocolCommand {
	RESERVE("BF.RESERVE"),
	ADD("BF.ADD"),
	MADD("BF.MADD"),
	EXISTS("BF.EXISTS"),
	MEXISTS("BF.MEXISTS"),
	DEL("DEL");
	private final byte[] raw;

	BFCommand(String alt) {
		raw = SafeEncoder.encode(alt);
	}

	@Override
	public byte[] getRaw() {
		return raw;
	}
}
