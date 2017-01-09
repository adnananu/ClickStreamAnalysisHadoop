package com.tut.fi;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
* we are doing our own shuffling. So we are taking only IP address because we gonna shuffle by IP
* in simple its for grouping the Ip'z
* */
public class IpKeyComparator extends WritableComparator {
    IpKeyComparator() {
        super(Ip_TimeStampKey.class, true);
    }

    @Override
    public int compare(WritableComparable rc_1, WritableComparable rc_2) {

        Ip_TimeStampKey iPkey_1 = (Ip_TimeStampKey) rc_1;//casting
        Ip_TimeStampKey iPkey_2 = (Ip_TimeStampKey) rc_2;//casting
        return iPkey_1.getIp().compareTo(iPkey_2.getIp());
    }
}
