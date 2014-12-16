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

import com.microsoft.reef.client.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import tbd.reef.param.*;

/**
 * TBD REEF application client.
 */
@Unit
public class TBDClient {

  /**
   * Standard java logger.
   */
  private static final Logger LOG =
      Logger.getLogger(TBDClient.class.getName());

  /**
   * Codec to translate messages to and from the job driver
   */
  private static final ObjectSerializableCodec<String> CODEC =
      new ObjectSerializableCodec<>();

  /**
   * Reference to the REEF framework.
   * This variable is injected automatically in the constructor.
   */
  private final REEF reef;

  /**
   * Job Driver configuration.
   */
  private final Configuration driverConfiguration;

  /**
   * If true, take commands from stdin; otherwise, in non-interactive mode.
   */
  private final boolean isInteractive;

  /**
   * Command prompt reader for the interactive mode (stdin).
   */
  private final BufferedReader prompt;

  /**
   * A reference to the running job that allows client to
   * send messages back to the job driver
   */
  private RunningJob runningJob;

  /**
   * Start timestamp of the current task.
   */
  private long startTime = 0;

  /**
   * Total time spent performing tasks in Evaluators.
   */
  private long totalTime = 0;

  /**
   * Number of experiments ran so far.
   */
  private int numRuns = 0;

  /**
   * Set to false when job driver is done.
   */
  public boolean isBusy = true;

  /**
   * Last result returned from the job driver.
   */
  private String lastResult;
  private String masterAkka = "unknown";
  private final int numWorkers;
  private final int timeout;

  /**
   * TBD REEF application client.
   * Parameters are injected automatically by TANG.
   *
   * @param reef    Reference to the REEF framework.
   */
  @Inject
  TBDClient(final REEF reef,
            @Parameter(TBDLaunch.NumWorkers.class) final Integer numWorkers,
            @Parameter(TBDLaunch.Timeout.class) final Integer timeout,
            @Parameter(Memory.class) final Integer memory,
            @Parameter(WorkerXmx.class) final Integer workerXmx,
            @Parameter(WorkerXss.class) final Integer workerXss)
                throws BindException {

    this.reef = reef;
    this.numWorkers = numWorkers;
    this.timeout = timeout;

    this.isInteractive = true;
    this.prompt = this.isInteractive ?
        new BufferedReader(new InputStreamReader(System.in)) : null;

    final JavaConfigurationBuilder configBuilder =
        Tang.Factory.getTang().newConfigurationBuilder();
    configBuilder.addConfiguration(
        DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES,
                TBDClient.class.getProtectionDomain()
                  .getCodeSource().getLocation().getFile())
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "TBDReefYarn")
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
                TBDDriver.EvaluatorAllocatedHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED,
                TBDDriver.EvaluatorFailedHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE,
                TBDDriver.ActiveContextHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_CLOSED,
                TBDDriver.ClosedContextHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_FAILED,
                TBDDriver.FailedContextHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING,
                TBDDriver.RunningTaskHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STARTED,
                TBDDriver.StartHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STOP,
                TBDDriver.StopHandler.class)
            .set(DriverConfiguration.ON_CLIENT_MESSAGE,
                TBDDriver.ClientMessageHandler.class)
            .build()
    );
    configBuilder.bindNamedParameter(TBDLaunch.NumWorkers.class,
        "" + numWorkers);
    configBuilder.bindNamedParameter(TBDLaunch.Timeout.class,
        "" + timeout);
    configBuilder.bindNamedParameter(Memory.class, "" + memory);
    configBuilder.bindNamedParameter(WorkerXmx.class, "" + workerXmx);
    configBuilder.bindNamedParameter(WorkerXss.class, "" + workerXss);
    this.driverConfiguration = configBuilder.build();
  }

  /**
   * @return a Configuration binding the ClientConfiguration
   *         event handlers to this Client.
   */
  public static Configuration getClientConfiguration() {
    return ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING,
            TBDClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE,
            TBDClient.JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED,
            TBDClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED,
            TBDClient.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR,
            TBDClient.RuntimeErrorHandler.class)
        .build();
  }

  /**
   * Launch the job driver.
   *
   * @throws BindException configuration error.
   */
  public void submit() {
    this.reef.submit(this.driverConfiguration);
  }

  public void processCmd(String cmd) {
    if (cmd.equals("help")){
      printList();
    } else if (cmd.equals("exit")) {
      //this.runningJob.send(CODEC.encode(cmd));
      this.runningJob.close();
      stopAndNotify();
    } else if (cmd.equals("master")) {
      System.out.println(masterAkka);
    } else if (cmd.equals("workers")) {
      this.runningJob.send(CODEC.encode(cmd));
    } else if (cmd.equals("add")) {
      System.out.println("Not implemented.");
    } else if (cmd.equals("remove")) {
      System.out.println("Not implemented.");
    } else {
      System.out.println("Illegal command.");
    }
  }

  private void printList() {
    System.out.println("Commands:");
    System.out.println("master - look up master");
    System.out.println("workers - look up workers");
    System.out.println("add - add a worker");
    System.out.println("remove - remove a worker");
    System.out.println("exit - terminate system");
  }

  /**
   * Notify the process in waitForCompletion() method that
   * the main process has finished.
   */
  private synchronized void stopAndNotify() {
    this.runningJob = null;
    this.isBusy = false;
    //this.notify();
  }

  /**
   * Wait for the job driver to complete. This method is called
   * from Launch.main()
   */
  public String waitForCompletion() {
    while (this.isBusy) {
      LOG.log(Level.FINE, "Waiting for the Job Driver to complete.");
      try {
        synchronized (this) {
          this.wait();
        }
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "Waiting for result interrupted.", ex);
      }
    }
    return this.lastResult;
  }

  public void close() {
    this.reef.close();
  }

  /**
   * Receive notification from the job driver that the job is running.
   */
  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.FINE, "Running job: {0}", job.getId());
      synchronized (TBDClient.this) {
        TBDClient.this.runningJob = job;
      }
    }
  }

  /**
   * Receive message from the job driver.
   * There is only one message, master's akka address.
   */
  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {
      String msg = CODEC.decode(message.get());
      if (msg.startsWith("akka")) {
        masterAkka = msg;
        System.out.println("master Akka:  " + masterAkka);
        System.out.println("");
      } else if (msg.startsWith("workers")) {
        System.out.println(msg);
      }
    }
  }

  /**
   * Receive notification from the job driver that the job had failed.
   */
  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      LOG.log(Level.SEVERE, "Failed job: " + job.getId(),
          job.getReason().orElse(null));
      stopAndNotify();
    }
  }

  /**
   * Receive notification from the job driver that
   * the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.FINE, "Completed job: {0}", job.getId());
      stopAndNotify();
    }
  }

  /**
   * Receive notification that there was an exception
   * thrown from the job driver.
   */
  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE,
          "Error in job driver: " + error, error.getReason().orElse(null));
      stopAndNotify();
    }
  }
}