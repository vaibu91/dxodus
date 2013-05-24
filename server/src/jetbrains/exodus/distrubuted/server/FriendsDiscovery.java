package jetbrains.exodus.distrubuted.server;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

public class FriendsDiscovery {

    private static FriendsDiscovery INSTANCE = new FriendsDiscovery();

    private DatagramSocket socket;

    private FriendsDiscovery() {
        try {
            socket = new DatagramSocket(3571, InetAddress.getByName("0.0.0.0"));
            socket.setBroadcast(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        listen();
    }

    public static FriendsDiscovery getInstance() {
        return INSTANCE;
    }

    private void listen() {
        new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("Listen on " + socket.getLocalAddress() + " from " + socket.getPort() + " port " + socket.getBroadcast());
                    byte[] buf = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    while (true) {
                        System.out.println("Waiting for data");
                        socket.receive(packet);
                        String data = new String(packet.getData(), 0, packet.getLength());
                        packet.getAddress();
                        System.out.println("Data received from: " + packet.getAddress() + " [" + data + "]");

                        if (URI.create(data).equals(App.getInstance().getBaseURI())) {
                            System.out.println("Do not make friends with myself");
                        } else {
                            // make friends
                            App.getInstance().addFriends(RemoteConnector.getInstance().friends(data, App.getInstance().getBaseURI().toString(), 1000));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    public void discoverFriends() {
        for (InetAddress bc : getBroadcastAddresses()) {
            byte[] data = App.getInstance().getBaseURI().toString().getBytes();
            DatagramPacket p = new DatagramPacket(data, data.length, bc, 3571);
            try {
                System.out.println("Send broadcast to " + bc);
                socket.send(p);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<InetAddress> getBroadcastAddresses() {
        ArrayList<InetAddress> listOfBroadcasts = new ArrayList();
        Enumeration list;
        try {
            list = NetworkInterface.getNetworkInterfaces();

            while(list.hasMoreElements()) {
                NetworkInterface iface = (NetworkInterface) list.nextElement();

                if(iface == null) continue;

                if(!iface.isLoopback() && iface.isUp()) {
                    System.out.println("Found non-loopback, up interface:" + iface);

                    Iterator it = iface.getInterfaceAddresses().iterator();
                    while (it.hasNext()) {
                        InterfaceAddress address = (InterfaceAddress) it.next();

                        if(address == null) continue;
                        InetAddress broadcast = address.getBroadcast();
                        if(broadcast != null) {
                            listOfBroadcasts.add(broadcast);
                            System.out.println("Found address: " + address);
                        }
                    }
                }
            }
        } catch (SocketException ex) {
            return new ArrayList<InetAddress>();
        }

        return listOfBroadcasts;
    }

}
