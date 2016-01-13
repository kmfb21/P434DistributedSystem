package com.resourcemonitor.daemon;

import com.resourcemonitor.common.MonitorBroker;
import com.resourcemonitor.common.MonitorData;

import javax.jms.JMSException;

import org.hyperic.sigar.Sigar;

/**
 * Common functionality for data collectors
 */
public abstract class AbstractDataCollector implements Runnable {
    // the sigar
    protected Sigar sigar;
    // the worker id
    protected int workerId;
    // the broker
    protected MonitorBroker broker;
    // time interval for collecting data
    private long time = 500;

    public AbstractDataCollector(int workerId, MonitorBroker broker, long sampleTime) {
        sigar = new Sigar();
        this.workerId = workerId;
        this.time = sampleTime;
        this.broker = broker;
    }

    // collect data and send to client
    public void run() {
        while (true) {
            MonitorData data = getMonitorData();
            sendToClient(data);
            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * The student should implement this method for Node data collector and Proc Data collector
     * @return monitor data
     */
    public abstract MonitorData getMonitorData();

    /**
     * Util function to send the data
     * @param data
     */
    private void sendToClient(MonitorData data) {
        if (data != null) {
            try {
				broker.send(data);
			} catch (JMSException e) {
				e.printStackTrace();
			}
        }
    }
}
