package com.resourcemonitor.daemon;

import com.resourcemonitor.common.MonitorBroker;
import com.resourcemonitor.common.MonitorData;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.ProcMem;
import org.hyperic.sigar.SigarException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Worker thread per node that sends
 * cpu and mem usage data.
 */
public class ProcDataCollector extends AbstractDataCollector {
	private long procIds[];

    /**
     * Constructor to create a process data collector
     * @param workerId daemon id
     * @param broker broker
     * @param sampleTime sampling time
     */
	public ProcDataCollector(int workerId, MonitorBroker broker, long sampleTime) {
		super(workerId, broker, sampleTime);

        Thread t = new Thread(new ProcessFinder());
        t.start();
	}

    /**
     * A thread that runs for getting the process ids
     */
    private class ProcessFinder implements Runnable {
        @Override
        public void run() {
            procIds = getProcIds();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Get process data using Sigar APIs. This function will be called time to time. This time is configured by
     * sample.time property.
     * The student needs to get the machine CPU usage and Memory usage using Sigar APIs,
     * create a MonitorData object and return
     *
     * If the procIds are not null go through them and collect data for these process IDs
     *
     * Please look at the <code>AbstractDataCollector</code> to get the sigar instance etc.
     *
     * @return a monitor data
     */
	public MonitorData getMonitorData() {
        MonitorData message = new MonitorData();
        if (this.procIds == null) {
            return null;
        }
        long mpiProcIds[] = this.procIds;
        /* implement your code here
        1. Get processes’ CPU and Mem using Sigar APIs. This function will be called time to time. This time is configured by sample.time in the monitor.properties.
        2. If the mpiProcIds are not null, go through them and collect performance data for these process IDs
        3. Calculate average CPU and Mem usage on current node
        */

        double allpmem = 0;
        double cpu = 0;
        for (long pid : mpiProcIds) {
            try {
            	//gather process cpu
                ProcCpu procCpu = new ProcCpu();
                procCpu.gather(sigar, pid);
                //get process memory
                ProcMem procMem = sigar.getProcMem(pid);
                allpmem += procMem.getSize() * 100 / sigar.getMem().getTotal();
                cpu += procCpu.getPercent();
            } catch (SigarException e) {
				e.printStackTrace();
			}
        }
    	message.setMemory(allpmem);
    	message.setCpu(cpu);
    	message.setProcess(true);
        
        return message;
    }

    /**
     * Student should implement this method to get the process IDs for the MPJ processes running in
     * the machine that daemon runs. You may need to run a shell command to get the process ids for the MPJ process.
     * for example to get the process ids for the Google chrome I can use the following command
     * ps ax | grep 'Chrome' | awk '{print $1}'
     * @return process IDs for MPJ process
     */
    private long[] getProcIds() {
        long list[] = null;
        /* implement your code here to get the MPJ processes’ pids
             e.g. to get the process ids for the Google chrome I can use the following command
     	ps ax | grep 'Chrome' | awk '{print $1}'
           1. call this command inside java program running under a linux environment.
           2. Parse the command result to a long list[] array.
        */
        
        Runtime run = Runtime.getRuntime();
        ArrayList<String> allproc = new ArrayList<String>();
		try {
			Process proc;
			//get mpj processes
			proc = run.exec(new String[]{"/bin/bash", "-c", "ps ax | grep \"mpj\" | awk '{print $1}'"});
			proc.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			//add command line result to allproc<String>
	        while(br.ready()) {
	        	allproc.add(br.readLine());
	        }
	        //save data from string to long and save them into list
	        list = new long[allproc.size()];
	        //and return it at last
	        for(int i = 0; i < allproc.size(); i++) {
	        	list[i] = Long.parseLong(allproc.get(i));
	        }
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        return list;
    }
}
