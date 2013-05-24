package jetbrains.exodus.distrubuted.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static List<InetAddress> getBroadcastAddresses() {
        ArrayList<InetAddress> listOfBroadcasts = new ArrayList<>();

        for (InterfaceAddress a: getAddresses()) {
            InetAddress b = a.getBroadcast();
            if (b != null) {
                listOfBroadcasts.add(b);
                log.info("Found broadcast " + b);
            }
        }

        return listOfBroadcasts;
    }

    public static InetAddress getLocalAddress() {
        for (InterfaceAddress a: getAddresses()) {
            if (a.getAddress() instanceof Inet4Address) {
                log.info("Found local address " + a.getAddress());
                return a.getAddress();
            }
        }

        return null;
    }

    public static List<InterfaceAddress> getAddresses() {
        ArrayList<InterfaceAddress> res = new ArrayList<>();
        Enumeration list;
        try {
            list = NetworkInterface.getNetworkInterfaces();

            while (list.hasMoreElements()) {
                NetworkInterface iface = (NetworkInterface) list.nextElement();

                if (iface == null) continue;

                if (!iface.isLoopback() && iface.isUp()) {
                    //log.info("Found non-loopback, up interface:" + iface);

                    Iterator it = iface.getInterfaceAddresses().iterator();
                    while (it.hasNext()) {
                        InterfaceAddress address = (InterfaceAddress) it.next();

                        if (address == null) continue;
                        res.add(address);
                        //log.info("Found address: " + address);
                    }
                }
            }
        } catch (SocketException ex) {
            return new ArrayList<>();
        }

        return res;
    }
}
