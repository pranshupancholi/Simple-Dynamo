package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    ConcurrentHashMap<String, String> dht = new ConcurrentHashMap<String, String>();
    ConcurrentHashMap<String, String> ret_data = new ConcurrentHashMap<String, String>();
    int thisNode;
    boolean flag = false;
    ArrayList<Integer> PORT_IDS = new ArrayList<Integer>();
    HashMap<Integer, String> PORT_IDS_HASH = new HashMap<Integer, String>();
    String portStr;
    boolean recovery = false, queryAllRec = false;
    int recoveryInfoReceivedFrom = 0, dataReceivedFrom = 0, prevPort;
    //boolean insertOngoing = false, queryOngoing = false;

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        thisNode = Integer.parseInt(portStr) * 2;
        Log.d(TAG, "Current Port NO " + thisNode/2);
        PORT_IDS.add(5562); PORT_IDS.add(5556); PORT_IDS.add(5554); PORT_IDS.add(5558); PORT_IDS.add(5560);

        PORT_IDS_HASH.put(5562, returnHash(String.valueOf(5562))); PORT_IDS_HASH.put(5556, returnHash(String.valueOf(5556)));
        PORT_IDS_HASH.put(5554, returnHash(String.valueOf(5554))); PORT_IDS_HASH.put(5558, returnHash(String.valueOf(5558)));
        PORT_IDS_HASH.put(5560, returnHash(String.valueOf(5560)));

        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "ServerSocket Creation Exception " + e.getMessage());
            return false;
        }

        SharedPreferences failCheck = this.getContext().getSharedPreferences("failCheck", 0);
        if (failCheck.getBoolean("initialStartUp", true)) {
            failCheck.edit().putBoolean("initialStartUp", false).commit();
        } else {
            recovery = true;
        }
        recoveryInfoReceivedFrom = 0;
        if(recovery) {
            Log.d(TAG, "Port "+thisNode/2+" Recovered");

            Message msg1 = new Message(findPred(thisNode/2), null, null, Message.MType.RECOVERY_REQUEST, thisNode/2, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);

            Message msg2 = new Message(findSucc(thisNode/2), null, null, Message.MType.RECOVERY_REQUEST, thisNode/2, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
        }

        return true;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*while(insertOngoing || queryOngoing || recovery){
            Log.d(TAG, "Insert IO: "+insertOngoing+" QO: "+queryOngoing+" REC: "+recovery);
        }*/

        //insertOngoing = true;
        //Log.d(TAG, "Insert Main Called");
        String key = values.getAsString("key");
        String value = values.getAsString("value");

        int cordinatorPort = findCordinatorPort(key);
        List<Integer> replicatorList = getReplicatorPorts(cordinatorPort, PORT_IDS);
        Log.d(TAG, "Key "+key+" Insert request forwarded to ports "+replicatorList);
        for(int portNo : replicatorList){
            Message msg = new Message(portNo, key, value, Message.MType.INSERT, thisNode/2, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }
        //insertOngoing = false;
        return uri;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        //Log.d(TAG, "Delete Main Called");
        if (selection.equals("@")) {
            dht.clear();
        } else if (selection.equals("*")) {
            dht.clear();
            for(int portNo : PORT_IDS) {
                if(portNo != thisNode) {
                    Message msg = new Message(portNo, selection, null, Message.MType.DELETE, 0, null);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
                }
            }
        } else {
            int cordinatorPort = findCordinatorPort(selection);
            List<Integer> replicatorList = getReplicatorPorts(cordinatorPort, PORT_IDS);
            Log.d(TAG, "Delete forwarded to ports " + replicatorList);
            for(int portNo : replicatorList) {
                Message msg = new Message(portNo, selection, null, Message.MType.DELETE, 0, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        }
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*while(insertOngoing || queryOngoing || recovery){
            Log.d(TAG, "Query IO: "+insertOngoing+" QO: "+queryOngoing+" REC: "+recovery);
        }*/
        synchronized (this) {
            //queryOngoing = true;
            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            if (selection.equals("@")) {
                Log.d(TAG, "Query @ called on " + thisNode);

                for (Entry<String, String> entry : dht.entrySet()) {
                    cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
            } else if (selection.equals("*")) {
                ret_data.putAll(dht);
                for (int portId : PORT_IDS) {
                    if (portId != thisNode / 2) {
                        Message msg = new Message(portId, "#", null, Message.MType.QUERY, thisNode / 2, null);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
                    }
                }
            /*Message msg = new Message(findSucc(thisNode/2), "*", null, Message.MType.QUERY, thisNode/2, dht);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);*/
                while (!queryAllRec) {
                    try {
                        Log.d(TAG, "dataReceivedFrom " + dataReceivedFrom);
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Log.d(TAG, "InterruptedException " + e.getMessage());
                    }
                }
                flag = false;
                queryAllRec = false;
                for (Entry<String, String> entry : ret_data.entrySet()) {
                    cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
                ret_data.clear();
            } else {
                Log.d(TAG, "Single Query Called for key " + selection);
                int cordinatorPort = findCordinatorPort(selection);
                List<Integer> replicatorList = getReplicatorPorts(cordinatorPort, PORT_IDS);

                Log.d(TAG, "Key found on replicators " + replicatorList);
            /*for(int portId : replicatorList) {
                Message msg = new Message(portId, selection, null, Message.MType.QUERY, thisNode/2, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }*/
                prevPort = replicatorList.get(1);
                Message msg = new Message(replicatorList.get(2), selection, null, Message.MType.QUERY, thisNode / 2, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

                while (!ret_data.containsKey(selection)) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Log.d(TAG, "InterruptedException " + e.getMessage());
                    }
                }
                Log.d(TAG, "Key Ret :" + selection + " Value Ret " + ret_data.get(selection));
                cursor.addRow(new String[]{selection, ret_data.get(selection)});
                ret_data.remove(selection);
            }
            //queryOngoing = false;
            return cursor;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //Log.d(TAG, "Inside Server");
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream objectInputStream =
                            new ObjectInputStream(socket.getInputStream());
                    Message message = (Message) objectInputStream.readObject();
                    //Log.d(TAG, "Server Cur port " + thisNode + " Pred port " + predNode + " " + "Succ Port" + succNode);
                    //Log.d(TAG, "Server Message MType " + message.type + " from Predecessor Port" + message.pred + " Succesor Port " + message.succ);

                    if (message.type == Message.MType.INSERT) {
                        Log.d(TAG, "Server Inserting key " + message.key + " to Port " + thisNode / 2 + " from " + message.pred);
                        dht.put(message.key, message.value);
                        //Log.d(TAG, "After Update key "+message.key+" value "+dht.get(message.key));
                    } else if (message.type == Message.MType.DELETE) {
                        if (message.key.equals("@")) {
                            dht.clear();
                        } else if (message.key.equals("*")) {
                            dht.clear();
                            Message msg = new Message(findSucc(thisNode/2), message.key, null, Message.MType.DELETE, thisNode/2, null);
                            invokeNewClientTask(msg);
                        } else {
                            dht.remove(message.key);
                        }
                    } else if (message.type == Message.MType.QUERY) {
                        if (message.key.equals("#")) {
                            Message msg = new Message(message.pred, null, null, Message.MType.QUERY_ALL, thisNode/2, dht);
                            invokeNewClientTask(msg);
                        } else {
                            //Log.d(TAG, "Inside QUERY ELSE IF");
                            Message msg = new Message(message.pred, message.key, dht.get(message.key), Message.MType.RET_MSG, thisNode/2, null);
                            invokeNewClientTask(msg);
                        }
                    } else if(message.type == Message.MType.QUERY_ALL) {
                        if(message.map != null)
                            ret_data.putAll(message.map);
                        if(++dataReceivedFrom == 4) {
                            queryAllRec = true;
                            dataReceivedFrom = 0;
                        }
                        Log.d(TAG, "dataReceivedFrom is "+dataReceivedFrom);
                    } else if (message.type == Message.MType.RET_MSG) {
                        if(ret_data != null && message.key != null && message.value != null) {
                            synchronized (ret_data) {
                                ret_data.put(message.key, message.value);
                            }
                        }
                    } else if (message.type == Message.MType.RECOVERY_REQUEST) {
                        Log.d(TAG, "Port "+thisNode/2+" sending recovery info to failed port "+message.pred);
                        Message msg = new Message(message.pred, null, null, Message.MType.DO_RECOVERY, thisNode/2, getRecoveryMap(message.pred));
                        invokeNewClientTask(msg);
                    } else if (message.type == Message.MType.DO_RECOVERY) {
                        Log.d(TAG, "Recovering Port "+thisNode/2+" Total Keys received "+ message.map.size()+" From "+message.pred);
                        dht.putAll(message.map);
                        if(++recoveryInfoReceivedFrom == 2) {
                            Log.d(TAG, "Port "+thisNode/2+" Fully Recovered");
                            recovery = false;
                        }
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "Server IO Read error: " + e.getMessage());
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "Server ClassNotFoundException " + e.getMessage());
                e.printStackTrace();
            }
            return null;
        }
    }

    public void invokeNewClientTask(Message msg) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }

    public ConcurrentHashMap<String, String> getRecoveryMap(int recoveredPort) {
        ConcurrentHashMap<String, String> recoveryMap = new ConcurrentHashMap<String, String>();
        for (Entry<String, String> entry : dht.entrySet()) {
            if(checkIfSkipped(entry.getKey(), recoveredPort))
                recoveryMap.put(entry.getKey(), entry.getValue());
        }
        Log.d(TAG, "Keys Recovered " + recoveryMap.size());
        return recoveryMap;
    }

    public boolean checkIfSkipped(String key, int recoveredPort){
        int cordinatorId = findCordinatorPort(key);
        List<Integer> replicaIDs = getReplicatorPorts(cordinatorId, PORT_IDS);

        return replicaIDs.contains(recoveredPort);
    }

    public String returnHash(String key) {
        String hashKey = null;
        try {
            hashKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Hash value generation exception " + e.getMessage());
        }
        return hashKey;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private List<Integer> getReplicatorPorts(int coordinatorId, List<Integer> portIds) {
        List<Integer> list = new ArrayList<Integer>();
        int cordIdx = -1;
        for (int i = 0; i < portIds.size(); i++)
            if (portIds.get(i) == coordinatorId)
                cordIdx = i;
        list.add(portIds.get(cordIdx));
        list.add(portIds.get((cordIdx + 1) % (portIds.size())));
        list.add(portIds.get((cordIdx + 2) % (portIds.size())));
        return list;
    }

    private int findCordinatorPort(String msgKey) {
        String hashKey = returnHash(msgKey);
        for (int portId : PORT_IDS)
            if (inMiddle(hashKey, portId))
                return portId;
        return 0;
    }

    private boolean inMiddle(String key, int portId) {
        String myHashedId = PORT_IDS_HASH.get(portId);

        int predPort = findPred(portId);
        String predId = PORT_IDS_HASH.get(predPort);

        if (myHashedId.compareTo(predId) == 0) {
            //Log.d(TAG, "Only 1 node in the ring");
            //Log.d(TAG, "RET 1");
            return true;
        }
        if ((key.compareTo(predId) > 0) && (key.compareTo(myHashedId) < 0) && (predId.compareTo(myHashedId) < 0)) {
            //Log.d(TAG, "RET 2");
            return true;
        } else if ((key.compareTo(predId) > 0) && (key.compareTo(myHashedId) > 0) && (predId.compareTo(myHashedId) > 0)) {
            //Log.d(TAG, "RET 3");
            return true;
        } else if ((key.compareTo(predId) < 0) && (key.compareTo(myHashedId) < 0) && (myHashedId.compareTo(predId) < 0)) {
            //Log.d(TAG, "RET 4");
            return true;
        }

        return false;
    }

    public int findPred(int portId){
        int nodeIndex = -1, predIdx;
        for (int i = 0; i < PORT_IDS.size(); i++)
            if (PORT_IDS.get(i) == portId)
                nodeIndex = i;
        if(nodeIndex > 0)
            predIdx = nodeIndex - 1;
        else
            predIdx = PORT_IDS.size() - 1;
        return PORT_IDS.get(predIdx);
    }

    public int findSucc(int portId){
        int nodeIndex = -1;
        for (int i = 0; i < PORT_IDS.size(); i++)
            if (PORT_IDS.get(i) == portId)
                nodeIndex = i;
        int succIdx = (nodeIndex + 1) % PORT_IDS.size();
        return PORT_IDS.get(succIdx);
    }

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message msg = msgs[0];
            try {
                Socket socket =
                        new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.fwdToPort*2);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                        socket.getOutputStream());
                objectOutputStream.writeObject(msg);
                objectOutputStream.close();
                socket.close();
                Log.d(TAG, "Inside Client Sent " + msg.type + " to " + msg.fwdToPort + " from: " + msg.pred);
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException " + e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException, Port " +msg.fwdToPort+" has failed");
                if(msg.type == Message.MType.QUERY && !msg.key.equals("#")) {
                    Message msg1 = new Message(prevPort, msg.key, null, Message.MType.QUERY, thisNode/2, null);
                    invokeNewClientTask(msg1);
                } else if(msg.type == Message.MType.QUERY && msg.key.equals("#"))
                    dataReceivedFrom++;

            }
            return null;
        }
    }
}