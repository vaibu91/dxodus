package jetbrains.exodus.distrubuted.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

public class FriendsDiscovery {

    private static final Logger log = LoggerFactory.getLogger(FriendsDiscovery.class);

    private static FriendsDiscovery INSTANCE = new FriendsDiscovery();

    private int[] ports = new int[]{3527, 3529, 3533, 3539, 3541};
    private DatagramSocket socket;

    private FriendsDiscovery() {
        for (int p : ports) {
            try {
                socket = new DatagramSocket(p, InetAddress.getByName("0.0.0.0"));
                socket.setBroadcast(true);
                log.info("Discovery started on port " + p);
                listen();

                return;
            } catch (Exception e) {
                //throw new RuntimeException(e);
                log.info("Can not start discovery on port " + p);
            }
        }

    }

    public static FriendsDiscovery getInstance() {
        return INSTANCE;
    }

    private void listen() {
        new Thread(new Runnable() {
            public void run() {
                try {
                    log.info("Listen on " + socket.getLocalAddress() + " from " + socket.getPort() + " port " + socket.getBroadcast());
                    byte[] buf = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    while (true) {
                        log.info("Waiting for data");
                        socket.receive(packet);
                        String data = new String(packet.getData(), 0, packet.getLength());
                        packet.getAddress();
                        log.info("Data received from: " + packet.getAddress() + " [" + data + "]");

                        if (URI.create(data).equals(App.getInstance().getBaseURI())) {
                            log.info("Do not make friends with myself");
                        } else {
                            // make friends
                            App.getInstance().addFriends(data);
                            // make friends from remote
                            final AsyncQuorum.Context<String[], String[]> ctx = AsyncQuorum.createContext(0, new AsyncQuorum.ResultFilter<String[], String[]>() {
                                @NotNull
                                @Override
                                public String[] fold(@Nullable String[] prev, @NotNull String[] current) {
                                    App.getInstance().addFriends(current);
                                    return current;
                                }
                            }, RemoteConnector.STRING_ARR_TYPE);
                            ctx.setFutures(
                                    RemoteConnector.getInstance().friendsAsync(data, App.getInstance().getBaseURI().toString(), ctx.getListener())
                            ).get();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "disco").start();
    }

    public void discoverFriends() {
        for (InetAddress bc : getBroadcastAddresses()) {
            final byte[] data = App.getInstance().getBaseURI().toString().getBytes();

            for (int p : ports) {
                DatagramPacket packet = new DatagramPacket(data, data.length, bc, p);
                try {
                    log.info("Send broadcast to " + bc + " on port " + p);
                    socket.send(packet);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private List<InetAddress> getBroadcastAddresses() {
        ArrayList<InetAddress> listOfBroadcasts = new ArrayList<>();
        Enumeration list;
        try {
            list = NetworkInterface.getNetworkInterfaces();

            while (list.hasMoreElements()) {
                NetworkInterface iface = (NetworkInterface) list.nextElement();

                if (iface == null) continue;

                if (!iface.isLoopback() && iface.isUp()) {
                    log.info("Found non-loopback, up interface:" + iface);

                    Iterator it = iface.getInterfaceAddresses().iterator();
                    while (it.hasNext()) {
                        InterfaceAddress address = (InterfaceAddress) it.next();

                        if (address == null) continue;
                        InetAddress broadcast = address.getBroadcast();
                        if (broadcast != null) {
                            listOfBroadcasts.add(broadcast);
                            log.info("Found address: " + address);
                        }
                    }
                }
            }
        } catch (SocketException ex) {
            return new ArrayList<>();
        }

        return listOfBroadcasts;
    }

}