package sdfs;


import sdfs.protocol.Url;

import java.util.*;

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

    /**
     * 负载均衡，选择合适的服务器服务
     * 懒得做，直接获取第一个^_^
     */
    public static Url chooseTarget(List<Url> urls) {
        if (urls != null && urls.size() > 0) {
            return urls.get(0);
        }
        return null;
    }
}
