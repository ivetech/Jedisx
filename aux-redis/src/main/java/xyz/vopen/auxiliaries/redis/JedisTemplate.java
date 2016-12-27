package xyz.vopen.auxiliaries.redis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Helper class that simplifies Redis data access code.
 * <p/>
 * Performs automatic serialization/deserialization between the given objects and the underlying binary data in the
 * Redis store. For String intensive operations consider the dedicated {@link JedisTemplate}.
 * <p/>
 * Once configured, this class is thread-safe.
 * <p/>
 * Note that while the template is generified, it is up to the serializers/deserializers to properly convert the given
 * Objects to and from binary data.
 * <p/>
 * <b>This is the central class in Redis support</b>.
 * </p>
 * <p>
 * 整合Spring-单机配置样例:<br/>
 * <pre>
 *      &lt;!--配置初始化JedisTemplate --&gt;
 *      &lt;bean id="jedisTemplate" class="xyz.vopen.auxiliaries.redis.JedisTemplate" init-method="init" destroy-method="destory"&gt;
 *
 *         &lt;!--
 *              是否开启懒加载配置
 *              <font color="red">true</font> - Redis在第一次使用Jedis Api操作缓存的时候进行初始化操作(耗时) ,不影响程序正常启动!
 *              <font color="red">false</font> - Redis在Spring Context初始化时候进行Redis初始化操作,如果Redis连接异常影响程序正常启动!
 *         --&gt;
 *         &lt;property name="check" value="false"/&gt;
 *
 *         &lt;!-- <font color="green">连接池公共配置</font> --&gt;
 *         &lt;property name="maxTotal" value="200"/&gt;
 *         &lt;property name="maxIdle" value="100"/&gt;
 *         &lt;property name="maxWaitMillis" value="5000"/&gt;
 *         &lt;property name="testOnBorrow" value="true"/&gt;
 *
 *         &lt;!--
 *              <font color="red"><b>单机配置详解</b></font>:
 *              mode - 单机配置固定值(single)  - 不能为空
 *              host - Redis的连接地址        - 不能为空
 *              port - Redis的端口 (默认为6379)
 *         --&gt;
 *
 *         &lt;property name="mode" value="single"/&gt;
 *         &lt;property name="host" value="192.168.0.34"/&gt;
 *         &lt;property name="port" value="6378"/&gt;
 *
 *      &lt;/bean&gt;
 * </pre>
 * </p>
 * <p>
 * <p>
 * 整合Spring-集群配置样例:<br/>
 * <pre>
 *      &lt;!--配置初始化JedisTemplate --&gt;
 *      &lt;bean id="jedisTemplate" class="xyz.vopen.auxiliaries.redis.JedisTemplate" init-method="init" destroy-method="destory"&gt;
 *
 *         &lt;!--
 *              是否开启懒加载配置
 *              <font color="red">true</font> - Redis在第一次使用Jedis Api操作缓存的时候进行初始化操作(耗时) ,不影响程序正常启动!
 *              <font color="red">false</font> - Redis在Spring Context初始化时候进行Redis初始化操作,如果Redis连接异常影响程序正常启动!
 *         --&gt;
 *         &lt;property name="check" value="false"/&gt;
 *
 *         &lt;!-- 连接池公共配置 --&gt;
 *         &lt;property name="maxTotal" value="200"/&gt;
 *         &lt;property name="maxIdle" value="100"/&gt;
 *         &lt;property name="maxWaitMillis" value="5000"/&gt;
 *         &lt;property name="testOnBorrow" value="true"/&gt;
 *
 *         &lt;!--
 *              <font color="red"><b>集群配置详解</b></font>:
 *                  mode - 单机配置固定值(sentinels)   - 不能为空
 *                  masters - sentinels集群监控的主节点名称 - 不能为空
 *                  sentinels - sentinels集群节点的地址列表 - 不能为空
 *         --&gt;
 *
 *         &lt;property name="mode" value="single"/&gt;
 *         &lt;property name="masters" value="master001"/&gt;
 *         &lt;property name="sentinels" value="192.168.20.2:26378,192.168.20.2:36378,192.168.20.2:46378"/&gt;
 *
 *      &lt;/bean&gt;
 * </pre>
 * </p>
 *
 * @author Elve.xu [iskp.me@gmail.com]
 * @version v1.2 - 14/12/2016.
 */
public class JedisTemplate {

    private final static Logger logger = LoggerFactory.getLogger(JedisTemplate.class);

    // ================================================   common config ===========================================================

    /**
     * Redis Config
     */
    private GenericObjectPoolConfig poolConfig;

    private Integer maxTotal = 200;
    private Integer maxIdle = 100;
    private Integer maxWaitMillis = 5000;
    private Boolean testOnBorrow = true;

    /**
     * 启动检查
     */
    private boolean check = false;

    /**
     * 是够已经加载了
     */
    private AtomicBoolean inited = new AtomicBoolean(false);

    /**
     * Start Mode (默认sentinels 模式)
     *
     * @see StartupMode
     */
    private String mode = StartupMode.sentinels.name();

    // ================================================   common config end ===================================================


    // ================================================ 单机节点配置 ===================================================
    //  ---- only use poolConfig ----

    /**
     * redis host
     */
    private String host;

    /**
     * default redis port
     */
    private Integer port = 6379;

    // ================================================ 单机节点配置  END ===================================================

    // ================================================   集群节点配置 ==========================================================
    /**
     * Master 节点名称
     */
    private String masters;

    /**
     * Sentinels Node <br/>
     * <code>192.168.20.31:26378,192.168.20.31:36378,192.168.20.31:46378</code>
     */
    private String sentinels;

    // ================================================   集群节点配置END ==========================================================


    /**
     * startup mode
     */
    public static enum StartupMode {

        single,

        sentinels;

        public static boolean validMode (String mode) {
            if (mode == null || mode.trim().length() == 0) {
                return false;
            }
            StartupMode[] modes = StartupMode.values();
            for (StartupMode mode1 : modes) {
                if (mode.equalsIgnoreCase(mode1.name())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * 操作连接池
     */
    private ShardedJedisSentinelPool shardedJedisSentinelPool;

    private SimpleJedisPool simpleJedisPool;


    public void init () {


        if (inited.compareAndSet(false, true)) { // 还未加载

            System.out.println("SEVERE:start init redis connection...");
            if (poolConfig == null) {
                poolConfig = new JedisPoolConfig();
                poolConfig.setMaxIdle(maxIdle);
                poolConfig.setMaxTotal(maxTotal);
                poolConfig.setMaxWaitMillis(maxWaitMillis);
                poolConfig.setTestOnBorrow(testOnBorrow);
            }

            switch (StartupMode.valueOf(this.mode)) {
                case sentinels:  // 集群模式
                    if (shardedJedisSentinelPool == null) {
                        try {
                            shardedJedisSentinelPool = new ShardedJedisSentinelPool(poolConfig, masters, sentinels);
                            System.out.println("SEVERE:Sentinels Redis inited success : " + shardedJedisSentinelPool);
                        } catch (Exception ignore) {
                            if (!check) {
                                System.out.println("SEVERE:Redis SentinelPool Init Error; ignore");
                            } else {
                                throw new JedisConnectionException("Redis初始化异常:" + ignore);
                            }
                        }
                    }
                    break;

                case single: // 单机模式

                    if (simpleJedisPool == null) {
                        try {
                            simpleJedisPool = new SimpleJedisPool(poolConfig, host, port);
                            if (simpleJedisPool.getResource() == null) {
                                throw new JedisException("Can't get Jedis Client from Pool");
                            }
                            System.out.println("SEVERE:Simple Redis Pool inited success : " + simpleJedisPool);

                        } catch (Exception ignore) {
                            if (!check) {
                                System.out.println("SEVERE:Redis SimplePool Init Error; ignore....");
                            } else {
                                throw new JedisConnectionException("Redis初始化异常:" + ignore);
                            }
                        }
                    }
                    break;
            }
        }
    }

    /**
     * Get JedisCommands Client
     *
     * @return return <code>null</code> of lose connection from redis server.
     */
    private JedisCommands getJedisResource () {

        try {

            switch (StartupMode.valueOf(this.mode)) {
                case sentinels:  // 集群模式

                    if (shardedJedisSentinelPool != null) {
                        return shardedJedisSentinelPool.getResource();
                    }

                    break;

                case single: // 单机模式

                    if (simpleJedisPool != null) {
                        return simpleJedisPool.getResource();
                    }

                    break;
            }

        } catch (JedisConnectionException e) {
            System.out.println("Lose Redis Connection ,Return NULL");
        } catch (JedisException e) {
            System.out.println("Lose Redis Connection ,Return NULL");
        }

        return null;
    }


    /**
     * simple jedis pool
     */
    private static class SimpleJedisPool extends JedisPool {

        public SimpleJedisPool (GenericObjectPoolConfig poolConfig, String host, int port) {
            super(poolConfig, host, port);
        }
    }


    /**
     * Redis Sentinel Pool
     */
    private static class ShardedJedisSentinelPool extends Pool<ShardedJedis> {

        private static final int MAX_RETRY_SENTINEL = 10;

//        private final java.util.logging.Logger log = java.util.logging.Logger.getLogger(getClass().getName());

        private GenericObjectPoolConfig poolConfig;

        private int timeout = Protocol.DEFAULT_TIMEOUT;

        private int sentinelRetry = 0;

        private String password;

        private Set<xyz.vopen.auxiliaries.redis.JedisTemplate.ShardedJedisSentinelPool.MasterListener> masterListeners = new HashSet<xyz.vopen.auxiliaries.redis.JedisTemplate.ShardedJedisSentinelPool.MasterListener>();

        private volatile List<HostAndPort> currentHostMasters;

        private Jedis jedis;

        /**
         * 初始化连接池
         *
         * @param poolConfig 连接池
         * @param masters    Master
         * @param sentinels  sents
         */
        public ShardedJedisSentinelPool (final GenericObjectPoolConfig poolConfig, String masters, String sentinels) {

            System.out.println("Start Create ShardedJedisSentinelPool(poolConfig, masters, sentinels) ..");
            List<String> _masters = new ArrayList<String>();
            Set<String> _sentinels = new HashSet<String>();

            if (masters != null && masters.length() > 0) {
                String[] tam = masters.split(",");
                if (tam.length > 0) {
                    Collections.addAll(_masters, tam);
                }
            }

            if (sentinels != null && sentinels.length() > 0) {
                String[] tam = sentinels.split(",");
                if (tam.length > 0) {
                    Collections.addAll(_sentinels, tam);
                }
            }

            this.poolConfig = poolConfig;
            this.timeout = Protocol.DEFAULT_TIMEOUT;
            this.password = null;

            List<HostAndPort> masterList = initSentinels(_sentinels, _masters);
            initPool(masterList);
        }

        public void destroy () {
            for (xyz.vopen.auxiliaries.redis.JedisTemplate.ShardedJedisSentinelPool.MasterListener m : masterListeners) {
                m.shutdown();
            }

            super.destroy();
        }

        private void initPool (List<HostAndPort> masters) {
            if (!equals(currentHostMasters, masters)) {
                StringBuffer sb = new StringBuffer();
                for (HostAndPort master : masters) {
                    sb.append(master.toString());
                    sb.append(" ");
                }
                logger.info("Created ShardedJedisPool to master at [" + sb.toString() + "]");
                List<JedisShardInfo> shardMasters = makeShardInfoList(masters);
                initPool(poolConfig, new xyz.vopen.auxiliaries.redis.JedisTemplate.ShardedJedisSentinelPool.ShardedJedisFactory(shardMasters, Hashing.MURMUR_HASH, null));
                currentHostMasters = masters;
            }
        }

        private boolean equals (List<HostAndPort> currentShardMasters, List<HostAndPort> shardMasters) {
            if (currentShardMasters != null && shardMasters != null) {
                if (currentShardMasters.size() == shardMasters.size()) {
                    for (int i = 0; i < currentShardMasters.size(); i++) {
                        if (!currentShardMasters.get(i).equals(shardMasters.get(i)))
                            return false;
                    }
                    return true;
                }
            }
            return false;
        }

        private List<JedisShardInfo> makeShardInfoList (List<HostAndPort> masters) {
            List<JedisShardInfo> shardMasters = new ArrayList<JedisShardInfo>();
            for (HostAndPort master : masters) {
                JedisShardInfo jedisShardInfo = new JedisShardInfo(master.getHost(), master.getPort(), timeout);
                jedisShardInfo.setPassword(password);

                shardMasters.add(jedisShardInfo);
            }
            return shardMasters;
        }

        private List<HostAndPort> initSentinels (Set<String> sentinels, final List<String> masters) {

            Map<String, HostAndPort> masterMap = new HashMap<String, HostAndPort>();
            List<HostAndPort> shardMasters = new ArrayList<HostAndPort>();

            logger.info("Trying to find all master from available Sentinels...");

            for (String masterName : masters) {
                HostAndPort master = null;
                boolean fetched = false;

                while (!fetched && sentinelRetry < MAX_RETRY_SENTINEL) {
                    for (String sentinel : sentinels) {
                        final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));

                        logger.info("Connecting to Sentinel " + hap);

                        try {
                            jedis = new Jedis(hap.getHost(), hap.getPort());
                            master = masterMap.get(masterName);
                            if (master == null) {
                                List<String> hostAndPort = jedis.sentinelGetMasterAddrByName(masterName);
                                if (hostAndPort != null && hostAndPort.size() > 0) {
                                    master = toHostAndPort(hostAndPort);
                                    logger.info("Found Redis master at " + master);
                                    shardMasters.add(master);
                                    masterMap.put(masterName, master);
                                    fetched = true;
                                    jedis.disconnect();
                                    break;
                                }
                            }
                        } catch (JedisConnectionException e) {
                            logger.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                        }
                    }

                    if (null == master) {
                        try {
                            logger.info("SEVERE:All sentinels down, cannot determine where is " + masterName
                                    + " master is running... sleeping 1000ms, Will try again.");
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        fetched = false;
                        sentinelRetry++;
                    }
                }

                // Try MAX_RETRY_SENTINEL times.
                if (!fetched && sentinelRetry >= MAX_RETRY_SENTINEL) {
                    logger.info("SEVERE:All sentinels down and try " + MAX_RETRY_SENTINEL + " times, Abort.");
                    throw new JedisConnectionException("Cannot connect all sentinels, Abort.");
                }
            }

            // All shards master must been accessed.
            if (masters.size() != 0 && masters.size() == shardMasters.size()) {

                logger.info("Starting Sentinel listeners...");
                for (String sentinel : sentinels) {
                    final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                    xyz.vopen.auxiliaries.redis.JedisTemplate.ShardedJedisSentinelPool.MasterListener masterListener = new xyz.vopen.auxiliaries.redis.JedisTemplate.ShardedJedisSentinelPool.MasterListener(masters, hap.getHost(), hap.getPort());
                    masterListeners.add(masterListener);
                    masterListener.start();
                }
            }

            return shardMasters;
        }

        private HostAndPort toHostAndPort (List<String> getMasterAddrByNameResult) {
            String host = getMasterAddrByNameResult.get(0);
            int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

            return new HostAndPort(host, port);
        }

        /**
         * PoolableObjectFactory custom impl.
         */
        protected static class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {
            private List<JedisShardInfo> shards;
            private Hashing algo;
            private Pattern keyTagPattern;

            ShardedJedisFactory (List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
                this.shards = shards;
                this.algo = algo;
                this.keyTagPattern = keyTagPattern;
            }

            public PooledObject<ShardedJedis> makeObject () throws Exception {
                ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
                return new DefaultPooledObject<ShardedJedis>(jedis);
            }

            public void destroyObject (PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
                final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
                for (Jedis jedis : shardedJedis.getAllShards()) {
                    try {
                        try {
                            jedis.quit();
                        } catch (Exception ignored) {

                        }
                        jedis.disconnect();
                    } catch (Exception ignored) {

                    }
                }
            }

            public boolean validateObject (PooledObject<ShardedJedis> pooledShardedJedis) {
                try {
                    ShardedJedis jedis = pooledShardedJedis.getObject();
                    for (Jedis shard : jedis.getAllShards()) {
                        if (!shard.ping().equals("PONG")) {
                            return false;
                        }
                    }
                    return true;
                } catch (Exception ex) {
                    return false;
                }
            }

            public void activateObject (PooledObject<ShardedJedis> p) throws Exception {

            }

            public void passivateObject (PooledObject<ShardedJedis> p) throws Exception {

            }
        }

        protected class JedisPubSubAdapter extends JedisPubSub {
            @Override
            public void onMessage (String channel, String message) {
            }

            @Override
            public void onPMessage (String pattern, String channel, String message) {
            }

            @Override
            public void onPSubscribe (String pattern, int subscribedChannels) {
            }

            @Override
            public void onPUnsubscribe (String pattern, int subscribedChannels) {
            }

            @Override
            public void onSubscribe (String channel, int subscribedChannels) {
            }

            @Override
            public void onUnsubscribe (String channel, int subscribedChannels) {
            }
        }

        protected class MasterListener extends Thread {

            List<String> masters;
            String host;
            int port;
            long subscribeRetryWaitTimeMillis = 5000;
            Jedis jedis;
            AtomicBoolean running = new AtomicBoolean(false);

            @SuppressWarnings("unused")
            protected MasterListener () {
            }

            MasterListener (List<String> masters, String host, int port) {
                this.masters = masters;
                this.host = host;
                this.port = port;
            }

            @SuppressWarnings("unused")
            public MasterListener (List<String> masters, String host, int port, long subscribeRetryWaitTimeMillis) {
                this(masters, host, port);
                this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
            }

            public void run () {

                running.set(true);

                while (running.get()) {

                    jedis = new Jedis(host, port);

                    try {
                        jedis.subscribe(new xyz.vopen.auxiliaries.redis.JedisTemplate.ShardedJedisSentinelPool.JedisPubSubAdapter() {
                            @Override
                            public void onMessage (String channel, String message) {
                                logger.info("Sentinel " + host + ":" + port + " published: " + message + ".");

                                String[] switchMasterMsg = message.split(" ");

                                if (switchMasterMsg.length > 3) {

                                    int index = masters.indexOf(switchMasterMsg[0]);
                                    if (index >= 0) {
                                        HostAndPort newHostMaster = toHostAndPort(
                                                Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
                                        List<HostAndPort> newHostMasters = new ArrayList<HostAndPort>();
                                        for (int i = 0; i < masters.size(); i++) {
                                            newHostMasters.add(null);
                                        }
                                        Collections.copy(newHostMasters, currentHostMasters);
                                        newHostMasters.set(index, newHostMaster);

                                        initPool(newHostMasters);
                                    } else {
                                        StringBuilder sb = new StringBuilder();
                                        for (String masterName : masters) {
                                            sb.append(masterName);
                                            sb.append(",");
                                        }
                                        logger.info("Ignoring message on +switch-master for master name " + switchMasterMsg[0]
                                                + ", our monitor master name are [" + sb + "]");
                                    }

                                } else {
                                    logger.info("SEVERE:Invalid message received on Sentinel " + host + ":" + port
                                            + " on channel +switch-master: " + message);
                                }
                            }
                        }, "+switch-master");

                    } catch (JedisConnectionException e) {

                        if (running.get()) {
                            logger.info("SEVERE:Lost connection to Sentinel at " + host + ":" + port
                                    + ". Sleeping 5000ms and retrying.");
                            try {
                                Thread.sleep(subscribeRetryWaitTimeMillis);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                        } else {
                            logger.info("Unsubscribing from Sentinel at " + host + ":" + port);
                        }
                    }
                }
            }

            void shutdown () {
                try {
                    logger.info("Shutting down listener on " + host + ":" + port);
                    running.set(false);
                    // This isn't good, the Jedis object is not thread safe
                    jedis.disconnect();
                } catch (Exception e) {
                    logger.info("SEVERE:Caught exception while shutting down: " + e.getMessage());
                }
            }
        }

    }


    /**
     * 设置一个key的过期时间（单位：秒）
     *
     * @param key     key值
     * @param seconds 多少秒后过期
     * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
     */
    public long expire (String key, int seconds) {
        if (key == null || key.equals("")) {
            return 0;
        }

        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.expire(key, seconds);
        } catch (Exception ex) {
            logger.error("EXPIRE error[key=" + key + " seconds=" + seconds
                    + "]" + ex.getMessage(), ex);
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return 0;
    }

    /**
     * 设置一个key在某个时间点过期
     *
     * @param key           key值
     * @param unixTimestamp unix时间戳，从1970-01-01 00:00:00开始到现在的秒数
     * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
     */
    public long expireAt (String key, int unixTimestamp) {
        if (key == null || key.equals("")) {
            return 0;
        }

        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.expireAt(key, unixTimestamp);
        } catch (Exception ex) {
            logger.error("EXPIRE error[key=" + key + " unixTimestamp="
                    + unixTimestamp + "]" + ex.getMessage(), ex);
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return 0;
    }

    /**
     * 截断一个List
     *
     * @param key   列表key
     * @param start 开始位置 从0开始
     * @param end   结束位置
     * @return 状态码
     */
    public String trimList (String key, long start, long end) {
        if (key == null || key.equals("")) {
            return "-";
        }
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.ltrim(key, start, end);
        } catch (Exception ex) {
            logger.error("LTRIM 出错[key=" + key + " start=" + start + " end="
                    + end + "]" + ex.getMessage(), ex);
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return "-";
    }


    /**
     * 获取列表(Default timeout 3s)
     *
     * @param key key
     * @return 返回列表
     */
    public List<String> blpop (String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.blpop(3, key);
        } catch (Exception ex) {
            logger.error("blpop error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return new ArrayList<String>();
    }

    /**
     * 检查Set长度
     *
     * @param key
     * @return
     */
    public long countSet (String key) {
        if (key == null) {
            return 0;
        }
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.scard(key);
        } catch (Exception ex) {
            logger.error("countSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return 0;
    }

    /**
     * 添加到Set中（同时设置过期时间）
     *
     * @param key     key值
     * @param seconds 过期时间 单位s
     * @param value
     * @return
     */
    public boolean addSet (String key, int seconds, String... value) {
        boolean result = addSet(key, value);
        if (result) {
            long i = expire(key, seconds);
            return i == 1;
        }
        return false;
    }

    /**
     * 添加到Set中
     *
     * @param key
     * @param value
     * @return
     */
    public boolean addSet (String key, String... value) {
        if (key == null || value == null) {
            return false;
        }
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.sadd(key, value);
            return true;
        } catch (Exception ex) {
            logger.error("setList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * @param key
     * @param value
     * @return 判断值是否包含在set中
     */
    public boolean containsInSet (String key, String value) {
        if (key == null || value == null) {
            return false;
        }
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.sismember(key, value);
        } catch (Exception ex) {
            logger.error("setList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * 获取Set
     *
     * @param key
     * @return
     */
    public Set<String> getSet (String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.smembers(key);
        } catch (Exception ex) {
            logger.error("getList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return new HashSet<String>();
    }

    /**
     * 从set中删除value
     *
     * @param key
     * @return
     */
    public boolean removeSetValue (String key, String... value) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.srem(key, value);
            return true;
        } catch (Exception ex) {
            logger.error("getList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * 从list中删除value 默认count 1
     *
     * @param key
     * @param values 值list
     * @return
     */
    public int removeListValue (String key, List<String> values) {
        return removeListValue(key, 1, values);
    }

    /**
     * 从list中删除value
     *
     * @param key
     * @param count
     * @param values 值list
     * @return
     */
    public int removeListValue (String key, long count,
                                List<String> values) {
        int result = 0;
        if (values != null && values.size() > 0) {
            for (String value : values) {
                if (removeListValue(key, count, value)) {
                    result++;
                }
            }
        }
        return result;
    }

    /**
     * 从list中删除value
     *
     * @param key
     * @param count 要删除个数
     * @param value
     * @return
     */
    public boolean removeListValue (String key, long count, String value) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.lrem(key, count, value);
            return true;
        } catch (Exception ex) {
            logger.error("getList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * 截取List
     *
     * @param key
     * @param start 起始位置
     * @param end   结束位置
     * @return
     */
    public List<String> rangeList (String key, long start, long end) {
        if (key == null || key.equals("")) {
            return null;
        }
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.lrange(key, start, end);
        } catch (Exception ex) {
            logger.error("rangeList 出错[key=" + key + " start=" + start
                    + " end=" + end + "]" + ex.getMessage(), ex);
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return new ArrayList<String>();
    }

    /**
     * 检查List长度
     *
     * @param key
     * @return
     */
    public long countList (String key) {
        if (key == null) {
            return 0;
        }
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.llen(key);
        } catch (Exception ex) {
            logger.error("countList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return 0;
    }

    /**
     * 添加到List中（同时设置过期时间）
     *
     * @param key     key值
     * @param seconds 过期时间 单位s
     * @param value
     * @return
     */
    public boolean addList (String key, int seconds, String... value) {
        boolean result = addList(key, value);
        if (result) {
            long i = expire(key, seconds);
            return i == 1;
        }
        return false;
    }

    /**
     * 添加到List
     *
     * @param key
     * @param value
     * @return
     */
    public boolean addList (String key, String... value) {
        if (key == null || value == null) {
            return false;
        }
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.lpush(key, value);
            return true;
        } catch (Exception ex) {
            logger.error("setList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * 添加到List(只新增)
     *
     * @param key
     */
    public boolean addList (String key, List<String> list) {
        if (key == null || list == null || list.size() == 0) {
            return false;
        }
        for (String value : list) {
            addList(key, value);
        }
        return true;
    }

    /**
     * 获取List
     *
     * @param key
     * @return
     */
    public List<String> getList (String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.lrange(key, 0, -1);
        } catch (Exception ex) {
            logger.error("getList error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    /**
     * 设置HashSet对象
     *
     * @param domain 域名
     * @param key    键值
     * @param value  Json String or String value
     * @return
     */
    public boolean setHSet (String domain, String key, String value) {
        if (value == null)
            return false;
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.hset(domain, key, value);
            return true;
        } catch (Exception ex) {
            logger.error("setHSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * 获得HashSet对象
     *
     * @param domain 域名
     * @param key    键值
     * @return Json String or String value
     */
    public String getHSet (String domain, String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hget(domain, key);
        } catch (Exception ex) {
            logger.error("getHSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return "";
    }

    /**
     * 删除HashSet对象
     *
     * @param domain 域名
     * @param key    键值
     * @return 删除的记录数
     */
    public long delHSet (String domain, String key) {
        JedisCommands jedis = null;
        long count = 0;
        try {
            jedis = getJedisResource();
            count = jedis.hdel(domain, key);
        } catch (Exception ex) {
            logger.error("delHSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return count;
    }

    /**
     * 删除HashSet对象
     *
     * @param domain 域名
     * @param key    键值
     * @return 删除的记录数
     */
    public long delHSet (String domain, String... key) {
        JedisCommands jedis = null;
        long count = 0;
        try {
            jedis = getJedisResource();
            count = jedis.hdel(domain, key);
        } catch (Exception ex) {
            logger.error("delHSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return count;
    }

    /**
     * 判断key是否存在
     *
     * @param domain 域名
     * @param key    键值
     * @return
     */
    public boolean existsHSet (String domain, String key) {
        JedisCommands jedis = null;
        boolean isExist = false;
        try {
            jedis = getJedisResource();
            isExist = jedis.hexists(domain, key);
        } catch (Exception ex) {
            logger.error("existsHSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return isExist;
    }

    /**
     * 全局扫描hset
     *
     * @param match field匹配模式
     * @return
     */
    public List<Map.Entry<String, String>> scanHSet (String domain,
                                                     String match) {
        JedisCommands jedis = null;
        try {
            int cursor = 0;
            jedis = getJedisResource();
            ScanParams scanParams = new ScanParams();
            scanParams.match(match);
            ScanResult<Map.Entry<String, String>> scanResult;
            List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>();
            do {
                scanResult = jedis.hscan(domain, String.valueOf(cursor),
                        scanParams);
                list.addAll(scanResult.getResult());
                cursor = Integer.parseInt(scanResult.getStringCursor());
            } while (cursor > 0);
            return list;
        } catch (Exception ex) {
            logger.error("scanHSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return new ArrayList();
    }


    /**
     * 返回 domain 指定的哈希集中所有字段的value值
     *
     * @param domain
     * @return
     */

    public List<String> hvals (String domain) {
        JedisCommands jedis = null;
        List<String> retList = null;
        try {
            jedis = getJedisResource();
            retList = jedis.hvals(domain);
        } catch (Exception ex) {
            logger.error("hvals error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return retList;
    }

    /**
     * 返回 domain 指定的哈希集中所有字段的key值
     *
     * @param domain
     * @return
     */

    public Set<String> hkeys (String domain) {
        JedisCommands jedis = null;
        Set<String> retList = null;
        try {
            jedis = getJedisResource();
            retList = jedis.hkeys(domain);
        } catch (Exception ex) {
            logger.error("hkeys error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return retList;
    }

    /**
     * 返回 domain 指定的哈希key值总数
     *
     * @param domain
     * @return
     */
    public long lenHset (String domain) {
        JedisCommands jedis = null;
        long retList = 0;
        try {
            jedis = getJedisResource();
            retList = jedis.hlen(domain);
        } catch (Exception ex) {
            logger.error("hkeys error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return retList;
    }

    /**
     * 设置排序集合
     *
     * @param key
     * @param score
     * @param value
     * @return
     */
    public boolean setSortedSet (String key, long score, String value) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.zadd(key, score, value);
            return true;
        } catch (Exception ex) {
            logger.error("setSortedSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * 获得排序集合
     *
     * @param key
     * @param startScore
     * @param endScore
     * @param orderByDesc
     * @return
     */
    public Set<String> getSoredSet (String key, long startScore,
                                    long endScore, boolean orderByDesc) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            if (orderByDesc) {
                return jedis.zrevrangeByScore(key, endScore, startScore);
            } else {
                return jedis.zrangeByScore(key, startScore, endScore);
            }
        } catch (Exception ex) {
            logger.error("getSoredSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return new HashSet();
    }

    /**
     * 计算排序长度
     *
     * @param key
     * @param startScore
     * @param endScore
     * @return
     */
    public long countSoredSet (String key, long startScore, long endScore) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            Long count = jedis.zcount(key, startScore, endScore);
            return count == null ? 0L : count;
        } catch (Exception ex) {
            logger.error("countSoredSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return 0L;
    }

    /**
     * 删除排序集合
     *
     * @param key
     * @param value
     * @return
     */
    public boolean delSortedSet (String key, String value) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            long count = jedis.zrem(key, value);
            return count > 0;
        } catch (Exception ex) {
            logger.error("delSortedSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    /**
     * 获得排序集合
     *
     * @param key
     * @param startRange
     * @param endRange
     * @param orderByDesc
     * @return
     */
    public Set<String> getSoredSetByRange (String key, int startRange,
                                           int endRange, boolean orderByDesc) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            if (orderByDesc) {
                return jedis.zrevrange(key, startRange, endRange);
            } else {
                return jedis.zrange(key, startRange, endRange);
            }
        } catch (Exception ex) {
            logger.error("getSoredSetByRange error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return new HashSet();
    }

    /**
     * 获得排序打分
     *
     * @param key
     * @return
     */
    public Double getScore (String key, String member) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zscore(key, member);
        } catch (Exception ex) {
            logger.error("getSoredSet error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public boolean set (String key, String value, int second) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.setex(key, second, value);
            return true;
        } catch (Exception ex) {
            logger.error("set error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    public boolean set (String key, String value) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.set(key, value);
            return true;
        } catch (Exception ex) {
            logger.error("set error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    public String get (String key, String defaultValue) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.get(key) == null ? defaultValue : jedis
                    .get(key);
        } catch (Exception ex) {
            logger.error("get error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return defaultValue;
    }

    public String get (String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.get(key);
        } catch (Exception ex) {
            logger.error("get error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return "";
    }

    public boolean del (String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            jedis.del(key);
            return true;
        } catch (Exception ex) {
            logger.error("del error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return false;
    }

    public long incr (String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.incr(key);
        } catch (Exception ex) {
            logger.error("incr error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return 0;
    }

    public long decr (String key) {
        JedisCommands jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.decr(key);
        } catch (Exception ex) {
            logger.error("incr error:{}", ex.getMessage());
            returnResource(jedis);
        } finally {
            returnResource(jedis);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    private void returnResource (ShardedJedis jedis) {
        try {
            jedis.close();
        } catch (Exception e) {
            logger.error("returnResource error:{}", e.getMessage());
        }
    }

    private void returnResource (JedisCommands jedis) {
        try {
            if (jedis != null) {
                if (jedis instanceof Jedis) {
                    ((Jedis) jedis).close();
                }

                if (jedis instanceof ShardedJedis) {
                    ((ShardedJedis) jedis).close();
                }
            }
        } catch (Exception e) {
        }
    }

    public void destory () {
        try {
            if (shardedJedisSentinelPool != null) {
                this.shardedJedisSentinelPool.destroy();
            }
        } catch (Exception e) {

        }

        try {
            if (simpleJedisPool != null) {
                this.simpleJedisPool.destroy();
            }
        } catch (Exception e) {

        }
    }


    // ==================================== all setter methods =========================================
    public void setPoolConfig (GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public void setMaxTotal (Integer maxTotal) {
        this.maxTotal = maxTotal;
    }

    public void setMaxIdle (Integer maxIdle) {
        this.maxIdle = maxIdle;
    }

    public void setMaxWaitMillis (Integer maxWaitMillis) {
        this.maxWaitMillis = maxWaitMillis;
    }

    public void setTestOnBorrow (Boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    /**
     * only work on <code>sentinels</code> mode <br/>
     * <p/>
     * <pre>
     *     # your redis sentinel config file for masters
     *     sentinel monitor <font color="red">master001</font> 192.168.20.33 6378 2
     * </pre>
     * <p/>
     * <pre>
     *     You can config multi also like this :<br/>
     *     <code>master001,master002,master003...</code>
     * </pre>
     *
     * @param masters masters
     */
    public void setMasters (String masters) {
        this.masters = masters;
    }

    /**
     * only work on <code>sentinels</code> mode <br/>
     * <p/>
     * <pre>
     *     # your redis sentinel config file for masters
     *     port 26378
     * </pre>
     * <p/>
     * <pre>
     *     You can config multi sentinels also like this :<br/>
     *     <code>192.168.20.30:26378,192.168.20.31:26378,192.168.20.32:26378...</code>
     * </pre>
     *
     * @param sentinels sentinels
     */
    public void setSentinels (String sentinels) {
        this.sentinels = sentinels;
    }

    public void setHost (String host) {
        this.host = host;
    }

    public void setPort (Integer port) {
        this.port = port;
    }

    /**
     * 是否启动检查异常 Redis Pool
     *
     * @param check true lazy otherwise not
     */
    public void setCheck (boolean check) {
        this.check = check;
    }

    /**
     * Redis Config mode
     *
     * @param mode mode <code>sentinels</code> default mode<br/>
     *             <li>sentinels</li>
     *             <li>single</li>
     * @see StartupMode
     */
    public void setMode (String mode) {

        if (!StartupMode.validMode(mode)) {
            this.mode = StartupMode.sentinels.name();
        } else {
            this.mode = mode;
        }
    }

    public void setStartupMode (StartupMode mode) {
        if (mode == null) {
            this.mode = StartupMode.sentinels.name();
        }
        this.mode = mode.name();
    }
}
