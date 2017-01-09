package com.tut.fi;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* To do the secondary sorting on data entering the reducer based time stamps
*/
//java Beans
public class Ip_TimeStampKey implements WritableComparable<Ip_TimeStampKey> {

    private String ip;
    private Long unixTS;

    Ip_TimeStampKey() {
    }

    //setter for ip and times stamp
    Ip_TimeStampKey(String ip, Long unixTS) {
        this.ip = ip;
        this.unixTS = unixTS;
    }

    //Getter
    public String getIp() {
        return ip;
    }

    public Long getUnixTS() {
        return unixTS;
    }

    //comparing ip's first then time stamps
    public int compareTo(Ip_TimeStampKey ipTSKey) {
        int result = ip.compareTo(ipTSKey.ip);
        if (result == 0) {//if both ip's are same then go for time stamp
            result = unixTS.compareTo(ipTSKey.unixTS);
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, ip);
        dataOutput.writeLong(unixTS);
    }

    public void readFields(DataInput dataInput) throws IOException {
        ip = WritableUtils.readString(dataInput);
        unixTS = dataInput.readLong();
    }
}
