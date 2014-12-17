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

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.hadoop.mapred.TextInputFormat;

import tbd.reef.param.Memory;
import tbd.reef.param.WorkerXmx;
import tbd.reef.param.WorkerXss;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

@ClientSide
public class DataLoadingReefYarn {

  private static final Logger LOG =
      Logger.getLogger(DataLoadingReefYarn.class.getName());

  private static final int NUM_LOCAL_THREADS = 16;
  private static int NUM_SPLITS;
  private static String masterIp;
  private static String akka;

  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "false")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "10")
  public static final class Timeout implements Name<Integer> {
  }

  @NamedParameter(short_name = "input")
  public static final class Input implements Name<String> {
  }

  @NamedParameter(doc = "Number of partitions, i.e., number of workers",
      short_name = "partitions", default_value = "2")
  public static final class Partitions implements Name<Integer> {
  }

  @NamedParameter(doc = "This should be set to the same to chunkSizes in TBD",
      short_name = "chunkSizes", default_value = "1")
  public static final class ChunkSizes implements Name<Integer> {
  }

  @NamedParameter(
      doc = "Master Akka system address",
      short_name = "masterAkka",
      default_value = "akka.tcp://masterSystem0@127.0.0.1:2555/user/master")
  public static final class MasterAkka implements Name<String> {
  }

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
        .registerShortNameOfClass(DataLoadingReefYarn.Local.class)
        .registerShortNameOfClass(DataLoadingReefYarn.Timeout.class)
        .registerShortNameOfClass(DataLoadingReefYarn.Input.class)
        .registerShortNameOfClass(DataLoadingReefYarn.Partitions.class)
        .registerShortNameOfClass(DataLoadingReefYarn.ChunkSizes.class)
        .registerShortNameOfClass(Memory.class)
        .registerShortNameOfClass(WorkerXmx.class)
        .registerShortNameOfClass(WorkerXss.class)
        .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());

    final boolean isLocal =
        injector.getNamedInstance(DataLoadingReefYarn.Local.class);
    final int jobTimeout =
        injector.getNamedInstance(DataLoadingReefYarn.Timeout.class);
    final String inputDir =
        injector.getNamedInstance(DataLoadingReefYarn.Input.class);
    final int partitions =
        injector.getNamedInstance(DataLoadingReefYarn.Partitions.class);
    final int chunkSizes =
        injector.getNamedInstance(DataLoadingReefYarn.ChunkSizes.class);
    final int memory =
        injector.getNamedInstance(Memory.class);
    final int workerXmx =
        injector.getNamedInstance(WorkerXmx.class);
    final int workerXss =
        injector.getNamedInstance(WorkerXss.class);

    NUM_SPLITS = partitions;

    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running Data Loading on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Data Loading on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
    outerloop:
    while(e.hasMoreElements()) {
        NetworkInterface n = (NetworkInterface) e.nextElement();
        Enumeration<InetAddress> ee = n.getInetAddresses();
        while (ee.hasMoreElements()) {
            InetAddress i = (InetAddress) ee.nextElement();
            String ip = i.getHostAddress();
            LOG.log(Level.INFO, "IP of launching node: {0}", ip);
            String[] parts = ip.split("\\.");
            if (parts.length == 4 && !parts[0].equals("127")) {
              masterIp = ip;
              break outerloop;
            }
        }
    }
    akka = "akka.tcp://masterSystem0@"
           + masterIp + ":" + "2555" + "/user/master";

    final JavaConfigurationBuilder configBuilder =
        Tang.Factory.getTang().newConfigurationBuilder();

    final Configuration dataLoadConfiguration = 
        new DataLoadingRequestBuilder()
          .setMemoryMB(memory)
          //.setMemoryMB(3072)
          .setNumberOfCores(2)
          .setInputFormatClass(TextInputFormat.class)
          .setInputPath(inputDir)
          .setNumberOfDesiredSplits(NUM_SPLITS)
          .setDriverConfigurationModule(DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES,
                EnvironmentUtils.getClassLocation(DataLoadingReefYarn.class))
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE,
                DataLoadingDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED,
                DataLoadingDriver.TaskCompletedHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER,
                "DataLoadingREEF"))
          .build();

    configBuilder.addConfiguration(dataLoadConfiguration);
    configBuilder.bindNamedParameter(
        DataLoadingReefYarn.Partitions.class, "" + partitions);
    configBuilder.bindNamedParameter(
        DataLoadingReefYarn.ChunkSizes.class, "" + chunkSizes);
    configBuilder.bindNamedParameter(
        DataLoadingReefYarn.MasterAkka.class, "" + akka);
    configBuilder.bindNamedParameter(
        DataLoadingReefYarn.Timeout.class, "" + jobTimeout);
    configBuilder.bindNamedParameter(
        WorkerXmx.class, "" + workerXmx);
    configBuilder.bindNamedParameter(
        WorkerXss.class, "" + workerXss);

    String cp = DataLoadingReefYarn.class.getProtectionDomain()
        .getCodeSource().getLocation().getFile();
    LOG.log(Level.INFO, "cp: {0}", cp);
    ProcessBuilder pb = new ProcessBuilder(
        "java",
        //"-Xss4m",
        "-cp", cp,
        "tbd.master.Main",
        "-i", masterIp,
        "-p", "2555");
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
    catch (IOException ex) {
      LOG.log(Level.INFO, "master process IO exception");
    }
    System.out.println();
    System.out.println();
    System.out.println(akka);
    System.out.println();
    System.out.println();

    final LauncherStatus state =
        DriverLauncher.getLauncher(runtimeConfiguration).
        run(configBuilder.build(), jobTimeout * 60 *1000);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);

    LOG.log(Level.INFO, "end master");
    p.destroy();
  }
}