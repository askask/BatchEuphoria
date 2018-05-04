/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs.cluster.lsf

import de.dkfz.roddy.BEException
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.*
import de.dkfz.roddy.tools.*
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Factory for the management of LSF cluster systems.
 *
 *
 */
@CompileStatic
class LSFJobManager extends AbstractLSFJobManager {

    private static final String LSF_COMMAND_QUERY_STATES = "bjobs -a -o -hms -json \"jobid job_name stat user queue " +
            "job_description proj_name job_group job_priority pids exit_code from_host exec_host submit_time start_time " +
            "finish_time cpu_used run_time user_group swap max_mem runtimelimit sub_cwd " +
            "pend_reason exec_cwd output_file input_file effective_resreq exec_home slots error_file command dependency \""
    private static final String LSF_COMMAND_DELETE_JOBS = "bkill"

    private String getQueryCommand() {
        return LSF_COMMAND_QUERY_STATES
    }

    @Override
    Map<BEJobID, JobInfo> queryExtendedJobStateById(List<BEJobID> jobIds) {
        Map<BEJobID, JobInfo> queriedExtendedStates = [:]
        for (BEJobID id : jobIds) {
            Map<String, String> jobDetails = runBjobs([id]).get(id)
            queriedExtendedStates.put(id, queryJobInfo(jobDetails))
        }
        return queriedExtendedStates
    }

    @Override
    protected JobState parseJobState(String stateString) {
        JobState js = JobState.UNKNOWN

        if (stateString == "RUN")
            js = JobState.RUNNING
        if (stateString == "PSUSP")
            js = JobState.SUSPENDED
        if (stateString == "PEND")
            js = JobState.QUEUED
        if (stateString == "DONE")
            js = JobState.COMPLETED_SUCCESSFUL
        if (stateString == "EXIT")
            js = JobState.FAILED

        return js
    }

    LSFJobManager(BEExecutionService executionService, JobManagerOptions parms) {
        super(executionService, parms)
    }

    protected LSFCommand createCommand(BEJob job) {
        return new LSFCommand(this, job, job.jobName, [], job.parameters, job.parentJobIDs*.id, job.tool?.getAbsolutePath() ?: job.getToolScript())
    }

    @Override
    JobInfo parseJobInfo(String commandString) {
        return new LSFCommandParser(commandString).toJobInfo();
    }

    protected Map<BEJobID, JobState> queryJobStates(List<BEJobID> jobIDs) {
        runBjobs(jobIDs).collectEntries { BEJobID jobID, Object value ->
            JobState js = parseJobState(value["STAT"] as String)
            [(jobID): js]
        } as Map<BEJobID, JobState>

    }

    private Map<BEJobID, Map<String, String>> runBjobs(List<BEJobID> jobIDs) {
        StringBuilder queryCommand = new StringBuilder(getQueryCommand())

        // user argument must be passed before the job IDs
        if (isTrackingOfUserJobsEnabled)
            queryCommand << " -u $userIDForQueries "

        if (jobIDs && jobIDs.size() < 10) {
            queryCommand << " ${jobIDs*.id.join(" ")} "
        }

        ExecutionResult er = executionService.execute(queryCommand.toString())
        List<String> resultLines = er.resultLines

        Map<BEJobID, Map<String, String>> result = [:]

        if (er.successful) {
            if (resultLines.size() >= 1) {
                String rawJson = resultLines.join("\n")
                Object parsedJson = new JsonSlurper().parseText(rawJson)
                List records = (List) parsedJson.getAt("RECORDS")
                records.each {
                    BEJobID jobID = new BEJobID(it["JOBID"] as String)
                    Map<String, String> values = (it as Map<String, Object>).collectEntries{ k, v -> [(k as String): v as String] } as Map<String, String>
                    result.put(jobID, values)
                }
            }

        } else {
            String error = "Job status couldn't be updated. \n command: ${queryCommand} \n status code: ${er.exitCode} \n result: ${er.resultLines}"
            throw new BEException(error)
        }
        return result
    }

    private static LocalDateTime parseTime(String str) {
        DateTimeFormatter datePattern = DateTimeFormatter.ofPattern("MMM ppd HH:mm yyyy").withLocale(Locale.ENGLISH)
        LocalDateTime date = LocalDateTime.parse(str + " " + LocalDateTime.now().getYear(), datePattern)
        if (date > LocalDateTime.now()) {
            return date.minusYears(1)
        }
        return date
    }

    /**
     * Used by @getJobDetails to set JobInfo
     */
    private JobInfo queryJobInfo(Map<String, String> jobResult) {
        BEJobID jobID = new BEJobID(jobResult["JOBID"])

        Set<BEJobID> dependencies = jobResult["DEPENDENCY"] ?
                jobResult["DEPENDENCY"].tokenize(/&/).collect { new BEJobID(it.find(/\d+/)) } :
                null
        String queue = jobResult["QUEUE"] ?: null
        Duration runTime = catchAndLogExceptions {
            jobResult["RUN_TIME"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUN_TIME"] as String) : null
        }
        BufferValue swap = catchAndLogExceptions {
            jobResult["SWAP"] ? new BufferValue((jobResult["SWAP"]).find("\\d+"), BufferUnit.m) : null
        }
        BufferValue memory = catchAndLogExceptions {
            String unit = (jobResult["MAX_MEM"]).find("[a-zA-Z]+")
            BufferUnit bufferUnit
            if (unit == "Gbytes")
                bufferUnit = BufferUnit.g
            else
                bufferUnit = BufferUnit.m
            jobResult["MAX_MEM"] ? new BufferValue((jobResult["MAX_MEM"]).find("([0-9]*[.])?[0-9]+"), bufferUnit) : null
        }
        Duration runLimit = catchAndLogExceptions {
            jobResult["RUNTIMELIMIT"] ? parseColonSeparatedHHMMSSDuration(jobResult["RUNTIMELIMIT"]) : null
        }

        ResourceSet usedResources = new ResourceSet(
                memory,
                null,
                catchAndLogExceptions { jobResult["SLOTS"] ? jobResult["SLOTS"] as Integer : null },
                runTime,
                null,
                queue,
                null
        )

        ResourceSet askedResources = new ResourceSet(
                null,
                null,
                null,
                runLimit,
                null,
                queue,
                null
        )

        return new JobInfo(
                jobResult["JOB_NAME"] ?: null,
                jobResult["COMMAND"] ? new File(jobResult["COMMAND"]) : null,
                jobID,
                null,
                dependencies,
                askedResources,
                usedResources,
                jobResult["SUBMIT_TIME"] ?
                        catchAndLogExceptions { parseTime(jobResult["SUBMIT_TIME"]) } :
                        null,
                null,
                jobResult["START_TIME"] ?
                        catchAndLogExceptions { parseTime(jobResult["START_TIME"]) } :
                        null,
                jobResult["FINISH_TIME"] ?
                        catchAndLogExceptions { parseTime((jobResult["FINISH_TIME"]).substring(0, (jobResult["FINISH_TIME"] as String).length() - 2)) } :
                        null,
                jobResult["EXEC_HOST"] ? (jobResult["EXEC_HOST"]).split(":").toList() : null,
                jobResult["FROM_HOST"] ?: null,
                jobResult["JOB_PRIORITY"] ?: null,
                getBjobsFile(jobResult["OUTPUT_FILE"], jobID, "out"),
                getBjobsFile(jobResult["ERROR_FILE"], jobID, "err"),
                jobResult["INPUT_FILE"] ? new File(jobResult["INPUT_FILE"]) : null,
                jobResult["USER"] ?: null,
                jobResult["USER_GROUP"] ?: null,
                jobResult["EFFECTIVE_RESREQ"] ?: null,
                null,
                null,
                null,
                null,
                null,
                parseJobState(jobResult["STAT"]),
                null,
                null,
                jobResult["PEND_REASON"] ?: null,
                jobResult["EXEC_HOME"] ?: null,
                null,
                jobResult["PIDS"] ? (jobResult["PIDS"]).split(",").toList() : null,
                null,
                parseJobState(jobResult["STAT"]) == JobState.COMPLETED_SUCCESSFUL ? 0 :
                        (jobResult["EXIT_CODE"] ? Integer.valueOf(jobResult["EXIT_CODE"]) : null),
                jobResult["JOB_GROUP"] ?: null,
                jobResult["JOB_DESCRIPTION"] ?: null,
                jobResult["EXEC_CWD"] ?: null,
                null,
                jobResult["SUB_CWD"] ?: null,
                jobResult["PROJ_NAME"] ?: null,
                jobResult["CPU_USED"] ? parseColonSeparatedHHMMSSDuration(jobResult["CPU_USED"]) : null,
                runTime,
                null,
                null,
                null,
                null,
                null,
                null,
        )
    }

    private File getBjobsFile(String s, BEJobID jobID, String type) {
        if (!s) {
            return null
        } else if (executionService.execute("stat -c %F ${BashUtils.strongQuote(s)}").firstLine == "directory") {
            return new File(s, "${jobID.getId()}.${type}")
        } else {
            return new File(s)
        }
    }

    @Override
    protected ExecutionResult executeKillJobs(List<BEJobID> jobIDs) {
        String command = "${LSF_COMMAND_DELETE_JOBS} ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    protected ExecutionResult executeStartHeldJobs(List<BEJobID> jobIDs) {
        String command = "bresume ${jobIDs*.id.join(" ")}"
        return executionService.execute(command, false)
    }

    @Override
    String parseJobID(String commandOutput) {
        String result = commandOutput.find(/<[0-9]+>/)
        //ToDo 'Group <resUsers>: Pending job threshold reached. Retrying in 60 seconds...'
        if (result == null)
            throw new BEException("Could not parse raw ID from: '${commandOutput}'")
        String exID = result.substring(1, result.length() - 1)
        return exID
    }

    @Override
    String getSubmissionCommand() {
        return "bsub"
    }

    @Override
    String getQueryJobStatesCommand() {
        return null
    }

    @Override
    String getExtendedQueryJobStatesCommand() {
        return null
    }
}
