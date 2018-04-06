/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */
package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.config.ResourceSet
import groovy.transform.CompileStatic
import groovy.transform.Immutable
import groovy.transform.ToString
import groovy.transform.TupleConstructor

import java.time.Duration
import java.time.LocalDateTime

/**
 * Created by michael on 06.02.15.
 */
@CompileStatic
@ToString(includeNames=true)
@TupleConstructor
@Immutable
class JobInfo {
    final String jobName
    final File tool
    final BEJobID jobID
    final Map<String, String> environment
    final Set<BEJobID> dependencies

    final ResourceSet askedResources
    final ResourceSet usedResources

    final LocalDateTime submitTime
    final LocalDateTime eligibleTime // when all conditions like job dependencies full filled, it is qu
    final LocalDateTime startTime
    final LocalDateTime endTime

    final List<String> executionHosts
    final String submissionHost
    final String priority

    final File logFile
    final File errorLogFile
    final File inputFile

    final String user
    final String userGroup
    final String resourceReq // resource requirements
    final Integer startCount

    final String account
    final String server
    final String umask

    final String otherSettings
    final JobState jobState
    final String userTime //user time used
    final String systemTime //system time used
    final String pendReason
    final String execHome
    final String execUserName
    final List<String> pidStr
    final String pgidStr // Currently active process group ID in a job.
    final Integer exitCode // UNIX exit status of the job
    final String jobGroup
    final String description
    final String execCwd //Executed current working directory
    final String askedHostsStr
    final String cwd //Current working directory
    final String projectName

    final Duration cpuTime //Cumulative total CPU time in seconds of all processes in a job
    final Duration runTime //Time in seconds that the job has been in the run state
    final Duration timeUserSuspState //Suspended by its owner or the LSF administrator after being dispatched
    final Duration timePendState //Waiting in a queue for scheduling and dispatch
    final Duration timePendSuspState // Suspended by its owner or the LSF administrator while in PEND state
    final Duration timeSystemSuspState //Suspended by the LSF system after being dispatched
    final Duration timeUnknownState
    final LocalDateTime timeOfCalculation
}
