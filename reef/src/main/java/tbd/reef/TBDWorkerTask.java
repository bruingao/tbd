/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbd.reef;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.reef.task.Task;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import tbd.reef.param.*;

/**
 * A worker node.
 */
public final class TBDWorkerTask implements Task {
  private static final Logger LOG =
      Logger.getLogger(TBDWorkerTask.class.getName());
  private final int timeout;
  private final String hostIP;
  private final String hostPort;
  private final String masterAkka;
  private final String xmx;
  private final String xss;

  @Inject
  TBDWorkerTask(@Parameter(TBDDriver.HostIP.class) final String ip,
      @Parameter(TBDDriver.HostPort.class) final String port,
      @Parameter(TBDDriver.MasterAkka.class) final String master,
      @Parameter(TBDLaunch.Timeout.class) final int tout,
      @Parameter(WorkerXmx.class) final int workerXmx,
      @Parameter(WorkerXss.class) final int workerXss
      ) {
    hostIP = ip;
    hostPort = port;
    masterAkka = master;
    timeout = tout;
    xmx = (workerXmx == 0) ?
        "" : "-Xmx" + Integer.toString(workerXmx/1024) + "g";
    xss = "-Xss" + Integer.toString(workerXss) + "m";
  }

  @Override
  public final byte[] call(final byte[] memento) {
    LOG.log(Level.INFO, "start worker");
    LOG.log(Level.INFO, "IP: {0}", hostIP);
    LOG.log(Level.INFO, "port: {0}", hostPort);
    LOG.log(Level.INFO, "master akka: {0}", masterAkka);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "worker sleep interrupted");
    }

    String cp = TBDWorkerTask.class.getProtectionDomain()
        .getCodeSource().getLocation().getFile();
    LOG.log(Level.INFO, "cp: {0}", cp);
    
    ProcessBuilder pb = new ProcessBuilder(
        "java",
        xmx,
        xss,
        "-cp", cp,
        "tbd.worker.Main",
        "-i", hostIP,
        "-p", hostPort, masterAkka);

    LOG.log(Level.INFO, "pb");

    pb.redirectErrorStream(true);
    pb.inheritIO();
    pb.redirectErrorStream(true);
    
    Process p = null;
    try {
      LOG.log(Level.INFO, "before start");
      p = pb.start();
      LOG.log(Level.INFO, "after start");
    } catch (IOException e1) {
      LOG.log(Level.INFO, "worker start failed");
    }

    LOG.log(Level.INFO, "worker sleep");
    try {
      Thread.sleep(timeout*60*1000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "worker sleep interrupted");
    }

    p.destroy();

    LOG.log(Level.INFO, "end worker");
    return null;
  }
}
