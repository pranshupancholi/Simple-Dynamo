package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Message implements Serializable {
    String key;
    String value;
    int pred;
    MType type;
    int fwdToPort;
    ConcurrentHashMap<String, String> map = null;

    public Message(int fwdP, String ky, String val, MType typ, int pre, ConcurrentHashMap<String, String> m) {
        fwdToPort = fwdP;
        key = ky;
        value = val;
        pred = pre;
        type = typ;
        map = m;
    }
    enum MType {
        INSERT,
        INSERT_ACK,
        DELETE,
        QUERY,
        QUERY_ALL,
        RET_MSG,
        RECOVERY_REQUEST,
        DO_RECOVERY
    }
}