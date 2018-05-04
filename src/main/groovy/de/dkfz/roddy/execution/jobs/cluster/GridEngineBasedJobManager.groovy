package de.dkfz.roddy.execution.jobs.cluster

import com.google.common.collect.LinkedHashMultimap
import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.BEJobID
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.GenericJobInfo
import de.dkfz.roddy.execution.jobs.JobInfo
import de.dkfz.roddy.execution.jobs.JobManagerOptions
import de.dkfz.roddy.execution.jobs.JobState
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import groovy.util.slurpersupport.GPathResult

import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.regex.Matcher

@CompileStatic
abstract class GridEngineBasedJobManager<C extends Command> extends ClusterJobManager<C> {

    public static final String WITH_DELIMITER = '(?=(%1$s))'

    GridEngineBasedJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    @Override
    boolean getDefaultForHoldJobsEnabled() { return true }

    @Override
    String getSubmissionCommand() {
        return "qsub"
    }

    protected int getColumnOfJobID() {
        return 0
    }

    protected int getColumnOfJobState() {
        return 4
    }

    @Override
    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryJobStatesCommand())

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " " << jobIDs*.id.join(" ")
        }

        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, JobState> result = [:]

        if (!er.successful) {
            throw new BEException("The execution of ${queryCommand} failed.\n\t" + er.resultLines?.join("\n\t")?.toString())
        } else {
            if (resultLines.size() > 2) {

                for (String line : resultLines) {
                    line = line.trim()
                    if (line.length() == 0) continue
                    if (!RoddyConversionHelperMethods.isInteger(line.substring(0, 1)))
                        continue //Filter out lines which have been missed which do not start with a number.

                    String[] split = line.split("\\s+")
                    final int ID = getColumnOfJobID()
                    final int JOBSTATE = getColumnOfJobState()

                    BEJobID jobID = new BEJobID(split[ID])

                    JobState js = parseJobState(split[JOBSTATE])
                    result.put(jobID, js)
                }
            }
        }
        return result
    }

    @Override
    Map<BEJobID, JobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, JobInfo> queriedExtendedStates
        String qStatCommand = getExtendedQueryJobStatesCommand()
        qStatCommand += " " + jobIds.collect { it }.join(" ")

        if (isTrackingOfUserJobsEnabled)
            qStatCommand += " -u $userIDForQueries "

        ExecutionResult er = executionService.execute(qStatCommand.toString())

        if (er != null && er.successful) {
            queriedExtendedStates = this.processQstatOutputFromXML(er.resultLines.join("\n"))
        } else {
            throw new BEException("Extended job states couldn't be retrieved. \n Returned status code:${er.exitCode} \n ${qStatCommand.toString()} \n\t result:${er.resultLines.join("\n\t")}")
        }
        return queriedExtendedStates
    }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "qrls ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "qdel ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    /**
     * Reads qstat output
     * @param qstatOutput
     * @return output of qstat in a map with jobid as key
     */
    private static Map<String, Map<String, String>> processQstatOutputFromPlainText(String qstatOutput) {
        return qstatOutput.split(String.format(WITH_DELIMITER, "\n\nJob Id: ")).collectEntries {
            Matcher matcher = it =~ /^\s*Job Id: (?<jobId>\d+)\..*\n/
            def result = new HashMap()
            if (matcher) {
                result[matcher.group("jobId")] = it
            }
            result
        }.collectEntries { jobId, value ->
            // join multi-line values
            value = ((String) value).replaceAll("\n\t", "")
            [(jobId): value]
        }.collectEntries { jobId, value ->
            Map<String, String> p = ((String) value).readLines().
                    findAll { it.startsWith("    ") && it.contains(" = ") }.
                    collectEntries {
                        String[] parts = it.split(" = ")
                        new MapEntry(parts.head().replaceAll(/^ {4}/, ""), parts.tail().join(' '))
                    }
            [(jobId): p]
        } as Map<String, Map<String, String>>
    }

    private static LocalDateTime parseTime(String str) {
        return catchAndLogExceptions { Instant.ofEpochSecond(Long.valueOf(str)).atZone(ZoneId.systemDefault()).toLocalDateTime() }
    }

    /**
     * Reads the qstat output and creates JobInfo objects
     * @param resultLines - Input of ExecutionResult object
     * @return map with jobid as key
     */
    protected Map<BEJobID, JobInfo> processQstatOutputFromXML(String result) {
        Map<BEJobID, JobInfo> queriedExtendedStates = [:]
        if (result.isEmpty()) {
            return [:]
        }

        GPathResult parsedJobs = new XmlSlurper().parseText(result)

        for (job in parsedJobs.children()) {
            String jobIdRaw = job["Job_Id"] as String
            BEJobID jobID
            try {
                jobID = new BEJobID(jobIdRaw)
            } catch (Exception exp) {
                throw new BEException("Job ID '${jobIdRaw}' could not be transformed to BEJobID ")
            }
            Set<BEJobID> jobDependencies = (job["depend"] as GPathResult).isEmpty() ?
                    (job["depend"] as  String).find("afterok.*")?.findAll(/(\d+).(\w+)/) {
                        fullMatch, String beforeDot, afterDot -> return beforeDot
                    } :
                    null
            String jobName = job["Job_Name"] as String ?: null

            BufferValue mem = null
            Integer cores
            Integer nodes
            TimeUnit walltime = null
            String additionalNodeFlag

            def resourceList = job["Resource_List"]
            String resourcesListMem = resourceList["mem"]
            String resourcesListNoDect = resourceList["nodect"]
            String resourcesListNodes = resourceList["nodes"]
            String resourcesListWalltime = resourceList["walltime"]
            if (!(resourcesListMem as GPathResult).isEmpty())
                mem = catchAndLogExceptions { new BufferValue(Integer.valueOf((resourcesListMem as String).find(/(\d+)/)), BufferUnit.valueOf((resourcesListMem as String)[-2])) }
            if (!(resourcesListNoDect as GPathResult).isEmpty())
                nodes = catchAndLogExceptions { Integer.valueOf(resourcesListNoDect as String) }
            if (!(resourcesListNodes as GPathResult).isEmpty())
                cores = catchAndLogExceptions { Integer.valueOf((resourcesListNodes as String).find("ppn=.*").find(/(\d+)/)) }
            if (!(resourcesListNodes as GPathResult).isEmpty())
                additionalNodeFlag = catchAndLogExceptions { (resourcesListNodes as String).find(/(\d+):(\.*)/) { fullMatch, nCores, feature -> return feature } }
            if (!(resourcesListWalltime as GPathResult).isEmpty())
                walltime = catchAndLogExceptions { new TimeUnit(resourcesListWalltime as String) }

            BufferValue usedMem = null
            TimeUnit usedWalltime = null
            def resourcesUsed = job["resources_used"]
            String resourcedUsedMem = resourcesUsed["mem"]
            String resourcesUsedWalltime = resourcesUsed["walltime"]
            if (!(resourcedUsedMem as GPathResult).isEmpty())
                catchAndLogExceptions { usedMem = new BufferValue(Integer.valueOf((resourcedUsedMem as String).find(/(\d+)/)), BufferUnit.valueOf((resourcedUsedMem as String)[-2])) }
            if (!(resourcesUsedWalltime as GPathResult).isEmpty())
                catchAndLogExceptions { usedWalltime = new TimeUnit(resourcesUsedWalltime as String) }

            ResourceSet requested = new ResourceSet(null, mem, cores, nodes, walltime, null, job["queue"] as String ?: null, additionalNodeFlag)
            ResourceSet used = new ResourceSet(null, usedMem, null, null, usedWalltime, null, job["queue"] as String ?: null, null)

            JobInfo jobInfo = new JobInfo(
                    jobName,
                    null,
                    jobID,
                    null,
                    jobDependencies,
                    requested,
                    used,
                    job["qtime"] ? (parseTime(job["qtime"] as String)) : null,
                    job["etime"] ? (parseTime(job["etime"] as String)) : null,
                    job["start_time"] ? parseTime(job["start_time"] as String) : null,
                    job["comp_time"] ? parseTime(job["comp_time"] as String) : null,
                    job["exec_host"] as String ? [job["exec_host"] as String] : null, //TODO
                    job["submit_host"] as String ?: null,
                    job["Priority"] as String ?: null,
                    getQstatFile((job["Output_Path"] as String).replace("\$PBS_JOBID", jobIdRaw)),
                    getQstatFile((job["Error_Path"] as String).replace("\$PBS_JOBID", jobIdRaw)),
                    null,
                    job["euser"] as String ?: null,
                    job["egroup"] as String ?: null,
                    job["submit_args"] as String ?: null,
                    job["start_count"] ? catchAndLogExceptions { Integer.valueOf(job["start_count"] as String) } : null,
                    job["Account_Name"] as String ?: null,
                    job["server"] as String ?: null,
                    job["umask"] as String ?: null,
                    null,
                    parseJobState(job["job_state"] as String),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    job["exit_status"] ? catchAndLogExceptions { Integer.valueOf(job["exit_status"] as String) }: null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    job["resources_used"]["cput"] ? catchAndLogExceptions { parseColonSeparatedHHMMSSDuration(job["resources_used"]["cput"] as String) } : null,
                    job["total_runtime"] ? catchAndLogExceptions { Duration.ofSeconds(Math.round(Double.parseDouble(job["total_runtime"] as String)), 0) } : null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
            )

            queriedExtendedStates.put(jobID, jobInfo)
        }
        return queriedExtendedStates
    }

    private static File getQstatFile(String s) {
        if (!s) {
            return null
        } else if (s.startsWith("/")) {
            return new File(s)
        } else if (s =~ /^[\w-]+:\//) {
            return new File(s.replaceAll(/^[\w-]+:/, ""))
        } else {
            return null
        }
    }

    @Override
    void createDefaultManagerParameters(LinkedHashMultimap<String, String> parameters) {

    }
}
