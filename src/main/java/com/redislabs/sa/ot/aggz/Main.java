package com.redislabs.sa.ot.aggz;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;

/**
 * This program showcases how LUA can be used to address the need to measure counts of entries stored in SortedSets
 * The idea is - folks from various email addresses ping our restaurant asking if we are offering their favorite dish yet
 * We want to count how many such pings we get in total and in the last X seconds
 * The Set of whoIsHungry? is established first - then the members of that Set are examined using ZCOUNT and ZCARD
 * It is important to note that for this example to function - all the generated keys use the same routing value
 * ^ this allows the LUA script to execute against them all in the same shard/process
 *
 * You can control the time window (looking back from now) by using the --lookbackseconds argument
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600"
 * You can control how many keys show new pings by using the --keyquantity argument
 *  If you want to clear out the membersKey so that only the --keyquantity number of members is examined you can add the argument --resetmembers
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600 --keyquantity 120 --resetmembers true"
 * --entrycount controls how many pings to record in each of the member keys
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host localhost --port 6379 --readonly false --lookbackseconds 600 --keyquantity 12 --entrycount 10"
 *
 */
public class Main {

    static String host = "localhost";
    static int port = 6379;
    static String username = "default";
    static String password = "";
    static URI jedisURI = null;
    static boolean isReadOnly=false;
    static int lookBackSeconds = 3;
    static ConnectionHelper connectionHelper = null;
    static String membersKey = "{whoIsHungry?}";
    static int keyQuantity = 25;
    static int entriesToAddToKeys = 25;
    static boolean resetMembers = false;

    //
    public static void main(String [] args){
        if(args.length>0) {
            ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--host")) {
                int hostIndex = argList.indexOf("--host");
                host = argList.get(hostIndex + 1);
            }
            if (argList.contains("--port")) {
                int portIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(portIndex + 1));
            }
            if (argList.contains("--user")) {
                int userNameIndex = argList.indexOf("--user");
                username = argList.get(userNameIndex + 1);
            }
            if (argList.contains("--password")) {
                int passwordIndex = argList.indexOf("--password");
                password = argList.get(passwordIndex + 1);
            }
            if (argList.contains("--readonly")) {
                int index = argList.indexOf("--readonly");
                isReadOnly = Boolean.parseBoolean(argList.get(index + 1));
            }
            if (argList.contains("--resetmembers")) {
                int index = argList.indexOf("--resetmembers");
                resetMembers = Boolean.parseBoolean(argList.get(index + 1));
            }
            if (argList.contains("--lookbackseconds")) {
                int index = argList.indexOf("--lookbackseconds");
                lookBackSeconds = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--memberskey")) {
                int index = argList.indexOf("--memberskey");
                membersKey = argList.get(index + 1);
            }
            if (argList.contains("--keyquantity")) {
                int index = argList.indexOf("--keyquantity");
                keyQuantity = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--entrycount")) {
                int index = argList.indexOf("--entrycount");
                entriesToAddToKeys = Integer.parseInt(argList.get(index + 1));
            }

        }
        connectionHelper = new ConnectionHelper(ConnectionHelper.buildURI(host,port,username,password));
        testJedisConnection(host,port);
        if(!isReadOnly) {
            buildSetOfTargetKeys(membersKey,keyQuantity);
            writeZ(membersKey,entriesToAddToKeys);
        }
        showCaseCountingZ(membersKey,lookBackSeconds);
    }

    static void buildSetOfTargetKeys(String setName,int howmany){
        Pipeline jedisPipe = connectionHelper.getPipeline();
        if(resetMembers){
            String luaCleanup = "local cursor = 0 local keyNum = 0 repeat " +
                    "local res = redis.call('scan',cursor,'MATCH',KEYS[1]..'*') " +
                    "if(res ~= nil and #res>=0) then cursor = tonumber(res[1]) " +
                    "local ks = res[2] if(ks ~= nil and #ks>0) then " +
                    "for i=1,#ks,1 do " +
                    "local key = tostring(ks[i]) " +
                    "redis.call('UNLINK',key) end " +
                    "keyNum = keyNum + #ks end end until( cursor <= 0 ) return keyNum";
            jedisPipe.eval(luaCleanup,1,membersKey);
            jedisPipe.del(setName);
        }
        for(int x=0;x<howmany;x++){
            jedisPipe.sadd(setName,setName+"email"+x+"@email.com");
        }
        jedisPipe.sync();
    }

    //returns the set of keynames suitable for our use (would be a set targeting a specific grouping)
    static Set<String> getSetOfTargetKeys(String setName) {
        Pipeline jedisPipe = connectionHelper.getPipeline();
        jedisPipe.smembers(setName);
        List<Object> response = jedisPipe.syncAndReturnAll();
        Set<String> keySet = null;
        for (Object o : response) {
            if (o instanceof java.util.HashSet) {
                keySet = (Set<String>) o;
            }
        }
        return keySet;
    }

    private static void testJedisConnection(String host,int port) {
        System.out.println("\ntesting jedis connection using URI == "+host+":"+port);
        JedisPooled jedis = connectionHelper.getPooledJedis();
        System.out.println("Testing connection by executing 'DBSIZE' response is: "+ jedis.dbSize());
    }


    //For each member, we can write a SortedSet that tracks email-address-related ping events with timestamps
    //passed in is the routingValue which doubles as the keyname for the SetOfKeys
    static void writeZ(String keynameRoutingValue,int numberEntries) {
        Pipeline jedisPipe = connectionHelper.getPipeline();
        Set<String> rSet = getSetOfTargetKeys(keynameRoutingValue);
        long delta = 0;
        for (String targetKeyName : rSet) {
            for (int x = 0; x < numberEntries; x++) {
                delta = System.nanoTime() % 30000;
                double timestamp = System.currentTimeMillis() - delta;
                jedisPipe.zadd(targetKeyName, timestamp, timestamp + "");
            }
        }
        jedisPipe.sync();
    }

        //
    static void showCaseCountingZ(String keyNameRoutingValue,long secondsBackInTime) {
        //SortedSet API offers ZCARD and ZCOUNT:
        String luaScript = "local resultString = '' local ruleSetKey = ARGV[1] " +
                "local txTime = ARGV[2] local lookBackSeconds = ARGV[3] " +
                "local keyNames = redis.call('SMEMBERS',ruleSetKey) " +
                "if #{keyNames} > 0 then " +
                "local innerLoop = 1 " +
                "while #{keyNames[innerLoop]} > 0 " +
                "do resultString = resultString..' '" +
                "..keyNames[innerLoop]..' '..(redis.call('ZCARD',keyNames[innerLoop]))..' '" +
                "..(redis.call('ZCOUNT',keyNames[innerLoop],(txTime-(lookBackSeconds*1000)),txTime))" +
                " innerLoop=(innerLoop+1) end end return resultString";
        JedisPooled jedis = connectionHelper.getPooledJedis();
        double timestamp = System.currentTimeMillis();
        Object luaResponse = jedis.eval(luaScript,1,keyNameRoutingValue,keyNameRoutingValue,""+timestamp,""+lookBackSeconds);
        System.out.println("\nResults from Lua: [keyName] [totalCount] [countForTimeWindow]  \n"+luaResponse);
        System.out.println("\n\nrunning the lua script with SMEMBERS logic took "+(System.currentTimeMillis()-timestamp+" milliseconds"));

        System.out.println("The script gathered the data from "+jedis.scard(keyNameRoutingValue)+" keys");
        System.out.println("The time window addressed was "+lookBackSeconds+" seconds");
    }
}

class ConnectionHelper{

    final PooledConnectionProvider connectionProvider;
    final JedisPooled jedisPooled;

    /**
     * Used when you want to send a batch of commands to the Redis Server
     * @return Pipeline
     */
    public Pipeline getPipeline(){
        return  new Pipeline(jedisPooled.getPool().getResource());
    }

    /**
     * Assuming use of Jedis 4.3.1:
     * https://github.com/redis/jedis/blob/82f286b4d1441cf15e32cc629c66b5c9caa0f286/src/main/java/redis/clients/jedis/Transaction.java#L22-L23
     * @return Transaction
     */
    public Transaction getTransaction(){
        return new Transaction(jedisPooled.getPool().getResource());
    }

    /**
     * Obtain the default object used to perform Redis commands
     * @return JedisPooled
     */
    public JedisPooled getPooledJedis(){
        return jedisPooled;
    }

    /**
     * Use this to build the URI expected in this classes' Constructor
     * @param host
     * @param port
     * @param username
     * @param password
     * @return
     */
    public static URI buildURI(String host,int port,String username,String password){
        URI uri = null;
        try {
            if (!("".equalsIgnoreCase(password))) {
                uri = new URI("redis://" + username + ":" + password + "@" + host + ":" + port);
            } else {
                uri = new URI("redis://" + host + ":" + port);
            }
        } catch (URISyntaxException use) {
            use.printStackTrace();
            System.exit(1);
        }
        return uri;
    }


    public ConnectionHelper(URI uri){
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        JedisClientConfig clientConfig = null;
        System.out.println("$$$ "+uri.getAuthority().split(":").length);
        if(uri.getAuthority().split(":").length==3){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1];
            password = password.split("@")[0];
            System.out.println("\n\nUsing user: "+user+" / password @@@@@@@@@@"+password);
            clientConfig = DefaultJedisClientConfig.builder().user(user).password(password)
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings

        }else {
            clientConfig = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings
        }
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(1000);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWait(Duration.ofMinutes(1));
        poolConfig.setTestOnCreate(true);

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
    }
}
