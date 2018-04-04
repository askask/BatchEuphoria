package de.dkfz.be.cli

import de.dkfz.roddy.AvailableClusterSystems
import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.BEJob
import de.dkfz.roddy.execution.jobs.BEJobResult
import de.dkfz.roddy.execution.jobs.BatchEuphoriaJobManager
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.tools.BufferValue
import groovy.transform.CompileStatic
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.ParentCommand

import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.time.Duration
import java.util.concurrent.Callable

import static java.lang.System.err
import static java.lang.System.exit

@CompileStatic
class BEExecutor {
    static void main(String... args) {

        CommandLine commandLine = new CommandLine(new Bee())

        List<ExecutionResult> result = commandLine.parseWithHandler(new CommandLine.RunFirst(), args) as List<ExecutionResult>
        result.first().with {
            println standardOutput
            err.println standardError
            exit exitCode
        }
    }
}
/*
 * TODO further subcommands:
 * start-held-jobs
 * kill-jobs
 * get-job-state
 * get-job-info
 * get-queues
 * get-hosts
 */


class BEVersion implements CommandLine.IVersionProvider {
    @Override
    String[] getVersion() throws Exception {
        return [getClass().getPackage().getImplementationVersion() ?: "UNKNOWN"]
    }
}

@Command(
        name = "batchee",
        description = "Execute cluster stuff.",
        mixinStandardHelpOptions = true,
        versionProvider = BEVersion,
        subcommands = [
                CommandLine.HelpCommand,
                BeeSubmitJob,
        ]
)
@CompileStatic
class Bee implements Callable<ExecutionResult> {
    @Option(names = ["--user-group"], paramLabel = "GROUP", description = "")
    String userGroup
    @Option(names = ["--user-account"], paramLabel = "ACCOUNT", description = "")
    String userAccount
    @Option(names = ["--user-email"], paramLabel = "EMAIL", description = "")
    String userEmail
    @Option(names = ["--user-mask"], paramLabel = "UMASK", description = "")
    String userMask

    @Option(names = ["--disable-memory-request"], description = "")
    boolean requestMemoryIsEnabled
    @Option(names = ["--disable-walltime-request"], description = "")
    boolean requestWalltimeIsEnabled = true
    @Option(names = ["--disable-queue-request"], description = "")
    boolean requestQueueIsEnabled
    @Option(names = ["--disable-cores-request"], description = "")
    boolean requestCoresIsEnabled
    @Option(names = ["--disable-storage-request"], description = "")
    boolean requestStorageIsEnabled
    @Option(names = ["--pass-environment"], description = "")
    boolean passEnvironment
    @Option(names = ["--hold-jobs"], description = "")
    Boolean holdJobIsEnabled

    @Option(names = ["--host"], paramLabel = "HOST", description = "")
    String host // or "localhost"
    //@Option(names = ["--port"], paramLabel = "PORT", description = "")
    //Integer port
    @Option(names = ["--user"], paramLabel = "USER", description = "")
    String user
    //@Option(names = ["--auth"], paramLabel = "METHOD", description = "")
    //Auth auth

    enum Auth {
        SSH_AGENT,
        PASSWORD,
        KEY,
    }

    @Option(names = ["--scheduler"], paramLabel = "SCHEDULER", description = "")
    AvailableClusterSystems cluster

    BatchEuphoriaJobManager getJobManager() throws IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException, NoSuchMethodException {
        JobManagerOptions options = JobManagerOptions.create()
                .setUserGroup(userGroup)
                .setUserAccount(userAccount)
                .setUserEmail(userEmail)
                .setUserMask(userMask)
                .setRequestMemoryIsEnabled(requestMemoryIsEnabled)
                .setRequestWalltimeIsEnabled(requestWalltimeIsEnabled)
                .setRequestQueueIsEnabled(requestQueueIsEnabled)
                .setRequestCoresIsEnabled(requestCoresIsEnabled)
                .setRequestStorageIsEnabled(requestStorageIsEnabled)
                .setPassEnvironment(passEnvironment)
                .setHoldJobIsEnabled(holdJobIsEnabled)
                .build()

        Constructor<?> c = Class.forName(cluster.getClassName()).getConstructor(BEExecutionService, JobManagerOptions)
        BEExecutionService executionService = new TestExecutionService(user, host) //TODO
        return (BatchEuphoriaJobManager) c.newInstance(executionService, options)
    }

    @Override
    ExecutionResult call() throws Exception {
        return new ExecutionResult(1, "No command given\nUse --help to see possible commands.", "")
    }
}


@Command(
        name = "submit-job",
        description = "Submit job"
)
@CompileStatic
class BeeSubmitJob implements Callable<ExecutionResult> {
    @Option(names = ["--jobname"], paramLabel = "JOBNAME", description = "job name")
    String jobName

    @Option(names = ["--tool"], paramLabel = "/PATH/TO/TOOL", description = "")//TODO
    File tool

    @Option(names = ["--request-memory"], paramLabel = "", description = "")
    BufferValue memory
    @Option(names = ["--request-cores"], paramLabel = "", description = "")
    Integer cores
    @Option(names = ["--request-nodes"], paramLabel = "", description = "")
    Integer nodes
    @Option(names = ["--request-walltime"], paramLabel = "", description = "")
    Duration walltime
    @Option(names = ["--request-storage"], paramLabel = "", description = "")
    BufferValue storage
    @Option(names = ["--request-queue"], paramLabel = "", description = "")
    String queue

    @Option(names = ["--dependencies"], paramLabel = "JOBID", description = "", arity = "1..*")
    Collection<BEJob> parentJobs
    @Option(names = ["--environment"], paramLabel = "A=B", description = "", arity = "1..*")
    Map<String, String> parameters
    //@Option(names = ["--job-log"], paramLabel = "TODO", description = "")
    //JobLog jobLog
    @Option(names = ["--working-directory"], paramLabel = "DIRECTORY", description = "")
    File workingDirectory

    @ParentCommand
    private Bee parent

    @Override
    ExecutionResult call() throws Exception {
        BatchEuphoriaJobManager jobManager = parent.getJobManager()

        ResourceSet resourceSet = new ResourceSet(
                memory, cores, nodes, walltime, storage, queue, null
        )

        BEJob job = new BEJob(
                null,
                jobName,
                tool,
                null, //TODO read from stdin
                null,
                resourceSet,
                parentJobs,
                parameters,
                jobManager,
                JobLog.none(), //TODO
                workingDirectory
        )

        BEJobResult s = jobManager.submitJob(job)
        if (s.successful) {
            new ExecutionResult(0, s.getJobID().getId(), "")
        } else{
            new ExecutionResult(1, "", "Job submission failed.")
        }
    }
}
