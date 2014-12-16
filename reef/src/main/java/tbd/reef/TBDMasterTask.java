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

/**
 * A master node.
 */
public final class TBDMasterTask implements Task {
  private static final Logger LOG =
      Logger.getLogger(TBDMasterTask.class.getName());
  private final int timeout;
  private final String masterIP;
  private final String masterPort;

  @Inject
  TBDMasterTask(@Parameter(TBDDriver.HostIP.class) final String ip,
      @Parameter(TBDDriver.HostPort.class) final String port,
      @Parameter(TBDLaunch.Timeout.class) final int tout) {
    masterIP = ip;
    masterPort = port;
    timeout = tout;
  }

  @Override
  public final byte[] call(final byte[] memento) {
    LOG.log(Level.INFO, "start master");
    LOG.log(Level.INFO, "master IP: {0}", masterIP);
    LOG.log(Level.INFO, "master port: {0}", masterPort);

    String cp = TBDMasterTask.class.getProtectionDomain()
        .getCodeSource().getLocation().getFile();
    LOG.log(Level.INFO, "cp: {0}", cp);

    ProcessBuilder pb = new ProcessBuilder(
        "java",
        "-cp", cp,
        "tbd.master.Main",
        "-i", masterIP,
        "-p", masterPort);

    LOG.log(Level.INFO, "pb");

    pb.redirectErrorStream(true);
    pb.inheritIO();
    pb.redirectErrorStream(true);

    Process p = null;
    try {
      LOG.log(Level.INFO, "before start");
      p = pb.start();
      LOG.log(Level.INFO, "after start");
    }
    catch (IOException e) {
      LOG.log(Level.INFO, "master process IO exception");
    }

    LOG.log(Level.INFO, "master sleep");
    try {
      Thread.sleep(timeout*60*1000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "master sleep interrupted");
    }

    p.destroy();

    LOG.log(Level.INFO, "end master");
    return null;
  }
}
