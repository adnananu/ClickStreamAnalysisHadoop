package com.tut.fi;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * this is used for whole sorting , mean both (ip address, timestamp), when records go to the reducer
 * are sorted in ascending order of timestamp for each ip address.
 */
public class EventKeyComparator extends WritableComparator {
    EventKeyComparator() {
        super(Ip_TimeStampKey.class, true);
    }

    @Override
    public int compare(WritableComparable rc_1, WritableComparable rc_2) {
        Ip_TimeStampKey iPkey_1 = (Ip_TimeStampKey) rc_1;
        Ip_TimeStampKey iPkey_2 = (Ip_TimeStampKey) rc_2;
        //comparing ip's first then time stamps
        int result = iPkey_1.getIp().compareTo(iPkey_2.getIp());
        if (result == 0) {//if both ip's are same then go for time stamp
            result = iPkey_1.getUnixTS().compareTo(iPkey_2.getUnixTS());
        }
        return result;
    }
}
