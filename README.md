# Jedisx
[![Maven Central](https://img.shields.io/badge/release-1.1.1--RELEASE-blue.svg)](https://github.com/ivetech/Jedisx)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.maven/apache-maven.svg)](http://search.maven.org/#artifactdetails%7Cio.github.ivetech.auxiliaries%7Caux-redis%7C1.1.1-RELEASE%7Cjar)
[![Twitter URL](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/@ytuaebi)

Simple JedisTemplate Client for Java ,Easy to use Anywhere ;Also Easy integrate with Spring Framework ;



# Redis Sentinels Mode


# Dependency

```xml
    <!-- add this jedis lib or not -->
    <dependency>
        <groupId>redis.clients</groupId>
        <artifactId>jedis</artifactId>
        <version>2.9.0</version>
    </dependency>
```



# How To Use

## Add Dependency
```xml
    <dependency>
        <groupId>io.github.ivetech.auxiliaries</groupId>
        <artifactId>aux-redis</artifactId>
        <version>RELEASE</version>
    </dependency>   
    
```


## Use With Java Application

```java
    @Test
    public void testSingleMode () {

        // new instance
        JedisTemplate jedisTemplate = new JedisTemplate();

        // set params
        jedisTemplate.setStartupMode(JedisTemplate.StartupMode.single);
        jedisTemplate.setHost("192.168.20.75");
        jedisTemplate.setPort(6379);
        jedisTemplate.setCheck(true);

        // init 
        jedisTemplate.init();

        // do sth.
        Assert.assertTrue(jedisTemplate.set("B", "B"));
        Assert.assertEquals("B", jedisTemplate.get("B"));

        // destroy 
        jedisTemplate.destory();
    }

```


## Use With Spring Integrate
```xml
    <!--配置初始化JedisTemplate -->
    <bean id="jedisTemplate" class="JedisTemplate" init-method="init" destroy-method="destory">

        <!--
            是否开启懒加载配置
            true - Redis在第一次使用Jedis Api操作缓存的时候进行初始化操作(耗时) ,不影响程序正常启动!
            false - Redis在Spring Context初始化时候进行Redis初始化操作,如果Redis连接异常影响程序正常启动!
         -->
        <property name="check" value="false"/>

        <!-- 连接池公共配置 -->
        <property name="maxTotal" value="200"/>
        <property name="maxIdle" value="100"/>
        <property name="maxWaitMillis" value="5000"/>
        <property name="testOnBorrow" value="true"/>


        <!--
            单机配置详解:
                mode - 单机配置固定值(single)  - 不能为空
                host - Redis的连接地址        - 不能为空
                port - Redis的端口 (默认为6379)
         -->

        <!--
        <property name="mode" value="single"/>
        <property name="host" value="hostname"/>
        <property name="port" value="6379"/>
        -->
        
        <!--
            集群配置详解:
                mode - 单机配置固定值(sentinels)   - 不能为空
                masters - sentinels集群监控的主节点名称 - 不能为空
                sentinels - sentinels集群节点的地址列表 - 不能为空
        -->
        <property name="mode" value="sentinels"/>
        <property name="masters" value="master001"/>
        <property name="sentinels" value="hostname:26378,hostname:36378,hostname:46378"/>

    </bean>
```
