package testsuite.util;

import testsuite.util.model.DbConfig;
import testsuite.util.model.DbInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
public class DbWorkRunnable implements Runnable {
    private Bussi bussi;


    public DbWorkRunnable(String params, DbConfig dbConfig) {
        this.bussi = new Bussi(params,dbConfig);
    }

    public DbWorkRunnable(String params) {
        this.bussi = new Bussi(params);
    }


    public DbWorkRunnable(Bussi bussi) {
        this.bussi = bussi;
    }

    @Override
    public void run() {
//        System.out.println("user: "+ bussi.dbConfig.getDb_name());


        if(bussi.getRunMode().equals(Bussi.ROMODE)){
            Bussi.runROBussi(bussi);
        }else{
            Bussi.runBussi(bussi);
        }
    }


    public Bussi getBussi() {
        return bussi;
    }
}
