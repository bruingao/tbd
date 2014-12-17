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
package tbd.reef.dataloading;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;

import tbd.reef.param.*;

@DriverSide
@Unit
public class DataLoadingDriver {
  private static final Logger LOG =
      Logger.getLogger(DataLoadingDriver.class.getName());

  private final AtomicInteger ctrlCtxIds = new AtomicInteger();
  private final AtomicInteger lineCnt = new AtomicInteger();
  private final AtomicInteger completedDataTasks = new AtomicInteger();

  private final DataLoadingService dataLoadingService;

  private int timeout;
  private int chunkSizes;
  private int partitions;
  private int workerXmx;
  private int workerXss;
  private String akka;
  private final Integer masterPort = 2555;

  private Map<String, ActiveContext> contexts =
      new HashMap<String, ActiveContext>();
  private Map<String, String> ctxt2ip = new HashMap<String, String>();
  private Map<String, Integer> ctxt2port = new HashMap<String, Integer>();

  @NamedParameter(doc = "IP address",
      short_name = "ip",
      default_value = "127.0.0.1")
  final class HostIP implements Name<String> {
  }

  @NamedParameter(doc = "port number",
      short_name = "port",
      default_value = "2555")
  final class HostPort implements Name<String> {
  }

  @Inject
  public DataLoadingDriver(
      final DataLoadingService dataLoadingService,
      @Parameter(DataLoadingReefYarn.Partitions.class) final int partitions,
      @Parameter(DataLoadingReefYarn.ChunkSizes.class) final int chunkSizes,
      @Parameter(DataLoadingReefYarn.MasterAkka.class) final String akka,
      @Parameter(DataLoadingReefYarn.Timeout.class) final int timeout,
      @Parameter(WorkerXmx.class) final int workerXmx,
      @Parameter(WorkerXss.class) final int workerXss
      ) {
    this.dataLoadingService = dataLoadingService;
    this.completedDataTasks.set(dataLoadingService.getNumberOfPartitions());
    this.timeout = timeout;
    this.partitions = partitions;
    this.chunkSizes = chunkSizes;
    this.workerXmx = workerXmx;
    this.workerXss = workerXss;
    this.akka = akka;
    LOG.log(Level.INFO, "partitions: {0}, chunkSizes: {1}",
        new Object[] { partitions, chunkSizes });
  }

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {

      final String contextId = activeContext.getId();
      LOG.log(Level.INFO, "Context active: {0}", contextId);

      if (dataLoadingService.isDataLoadedContext(activeContext)
          && !contexts.keySet().contains(contextId)) {

        contexts.put(contextId, activeContext);

        String socketAddr = activeContext.getEvaluatorDescriptor().
            getNodeDescriptor().getInetSocketAddress().toString();
        String ip = socketAddr.substring(
            socketAddr.indexOf("/")+1, socketAddr.indexOf(":"));
        Integer port = ctrlCtxIds.incrementAndGet() + masterPort;

        ctxt2ip.put(contextId, ip);
        ctxt2port.put(contextId, port);

        final String taskId = "Task-" + (port - masterPort);
        LOG.log(Level.INFO, "Submit task {0} to: {1}",
            new Object[] { taskId, contextId });

        try {
          final JavaConfigurationBuilder cb =
              Tang.Factory.getTang().newConfigurationBuilder();
          cb.addConfiguration(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, DataLoadingTask.class)
              .build());

          cb.bindNamedParameter(
              DataLoadingReefYarn.Partitions.class, "" + partitions);
          cb.bindNamedParameter(
              DataLoadingReefYarn.ChunkSizes.class, "" + chunkSizes);
          cb.bindNamedParameter(
              DataLoadingReefYarn.MasterAkka.class, "" + akka);
          cb.bindNamedParameter(
              DataLoadingReefYarn.Timeout.class, "" + timeout);
          cb.bindNamedParameter(
              DataLoadingDriver.HostIP.class, "" + ip);
          cb.bindNamedParameter(
              DataLoadingDriver.HostPort.class, "" + port);
          cb.bindNamedParameter(
              WorkerXmx.class, "" + workerXmx);
          cb.bindNamedParameter(
              WorkerXss.class, "" + workerXss);

          activeContext.submitTask(cb.build());

        } catch (final BindException ex) {
          LOG.log(Level.INFO, "Configuration error in " + contextId, ex);
          throw new RuntimeException("Configuration error in "
              + contextId, ex);
        }
      } else {
        LOG.log(Level.INFO, "Unrecognized context: {0}", contextId);
      }
    }
  }

  public class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {

      final String taskId = completedTask.getId();
      LOG.log(Level.INFO, "Completed Task: {0}", taskId);

      final byte[] retBytes = completedTask.get();
      final String retStr =
          retBytes == null ? "No RetVal": new String(retBytes);
      LOG.log(Level.INFO, "Line count from {0} : {1}",
          new String[] {taskId, retStr});

      lineCnt.addAndGet(Integer.parseInt(retStr));

      if (completedDataTasks.decrementAndGet() <= 0) {
        LOG.log(Level.INFO, "Total line count: {0}", lineCnt.get());
      }

      LOG.log(Level.INFO, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();
    }
  }
}