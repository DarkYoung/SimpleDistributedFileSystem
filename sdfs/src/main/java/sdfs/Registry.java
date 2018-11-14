package sdfs;


import sdfs.protocol.Url;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Registry {
    private final static Set<Url> registered;

    static {
        registered = new HashSet<>();
    }

    public static void register(Url url) {
        if (url != null && url.getService() != null && !url.getService().trim().equals("")) {
            System.out.println(url);
            registered.add(url);
        }
    }

    public static void unRegister(Url url) {
        if (url != null && url.getService() != null && !url.getService().trim().equals("")) {
            registered.remove(url);
//            System.out.println(url);
        }
    }

    /**
     * 获取所有提供url.getService()服务的Url
     */
    public static List<Url> lookupUrls(Url url) {
        if (url == null || url.getService() == null || url.getService().trim().equals("")) {
            return null;
        }
        List<Url> urls = new ArrayList<>();
        String service = url.getService();
        for (Url u : registered) {
            if (u.getService().equals(service))
                urls.add(u);
        }
        return urls;
    }

    public static Url getServerUrl(Url url) {
        List<Url> urls = lookupUrls(url);
        for (Url dst : urls) {
            if (dst.equals(url)) {
                return dst;
            }
        }
        return null;
    }

    /**
     * @return 第一个有效的服务
     */
    public static Url chooseFirst(List<Url> urls) {
        if (urls != null && urls.size() > 0) {
            return urls.get(0);
        }
        return null;
    }


    public static Url chooseTarget(Url src) {
        if(src.getHost() != null && !"".equals(src.getHost().trim()) && src.getPort() > 0)
            return chooseTarget(src, StrategyName.MATCH);
        return chooseTarget(src, StrategyName.RANDOM);
    }

    public static Url chooseTarget(Url src, StrategyName strategyName) {
        return chooseTarget(lookupUrls(src), src, strategyName);
    }

    public static Url chooseTarget(List<Url> urls, Url src) {
        return chooseTarget(urls, src, StrategyName.RANDOM);
    }

    public static Url chooseTarget(List<Url> urls, Url src, StrategyName strategyName) {
        Url url = null;
        try {
            Class<Strategy> c = (Class<Strategy>) Class.forName(strategyName.getName());
            Strategy strategy = (Strategy) c.newInstance();
            Method method = c.getMethod("getUrl", Url.class, List.class);
            Object obj = method.invoke(strategy, src, urls);
            if (obj instanceof Url) {
                url = (Url) obj;
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return chooseFirst(urls);
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return url;
    }

    public enum StrategyName {

        RANDOM(RandomStrategy.class.getName()), MATCH(MatchStrategy.class.getName());
        private String name;

        StrategyName(String s) {
            this.name = s;
        }

        public String getName() {
            return name;
        }
    }
}


/**
 * 负载均衡策略
 */
interface Strategy {
    Url getUrl(Url src, List<Url> urls);

}

/**
 * 随机选择
 */
class RandomStrategy implements Strategy {
    @Override
    public Url getUrl(Url src, List<Url> urls) {
        int size = urls.size();
        int index = 0;
        if (size > 0)
            index = (int) ((Math.random() * 1000) % size);
        return urls.get(index);
    }
}

/**
 * 匹配选择
 */

class MatchStrategy implements Strategy {

    @Override
    public Url getUrl(Url src, List<Url> urls) {
        for (Url dst : urls) {
            if (dst.equals(src)) {
                return dst;
            }
        }
        return null;
    }
}

