/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.BEException
import de.dkfz.roddy.TestExecutionService
import de.dkfz.roddy.config.JobLog
import de.dkfz.roddy.config.ResourceSet
import de.dkfz.roddy.execution.BEExecutionService
import de.dkfz.roddy.execution.RestExecutionService
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import groovy.transform.CompileStatic
import org.junit.BeforeClass
import org.junit.Test
import org.xml.sax.SAXParseException

import java.time.Duration

/**
 *
 * Only the tests are executed for which the cluster configuration are set in the "integrationTest.properties"
 *
 */
@CompileStatic
class BEIntegrationTest {
    static Properties properties
    static Map<ClusterScheduler, BEExecutionService> eServicesPerSystem = [:]
    static Map<ClusterScheduler, Boolean> testScriptWritten = [:]
    static String testScript = "ls"
    static File batchEuphoriaTestScript
    static JobLog logFile
    static ResourceSet resourceSet = new ResourceSet(new BufferValue(10, BufferUnit.m), 1, 1, Duration.ofMinutes(1), null, null, null)


    @BeforeClass
    static void readProperties() {
        properties = new Properties()
        File propertiesFile = new File('integrationTest.properties')
        propertiesFile.withInputStream {
            properties.load(it)
        }

        if (properties."logpath" != "")
            logFile = JobLog.toOneFile(new File(properties."logpath" as String))
        else
            logFile = JobLog.none()
    }

    static synchronized BEExecutionService getExecutionServiceFor(ClusterScheduler system) {
        if (!eServicesPerSystem[system]) {
            println(properties)
            def account = properties["${system}.account".toString()] as String
            def host = properties["${system}.host".toString()] as String
            if (system == ClusterScheduler.LSF_REST)
                eServicesPerSystem[system] = new RestExecutionService(host, account, properties."lsfrest.password" as String)
            else
                eServicesPerSystem[system] = new TestExecutionService(account, host)

        }
        return eServicesPerSystem[system]
    }

    static BatchEuphoriaJobManager createJobManagerFor(ClusterScheduler scheduler) {
        JobManagerOptions options = JobManagerOptions.create()
                .setCreateDaemon(false)
                .build()
        return BatchEuphoriaJobManager.create(scheduler, getExecutionServiceFor(scheduler), options)
    }

    void checkAndPossiblyRunJobWithPipedScript(ClusterScheduler system) {
        if (!(properties["${system}.host".toString()] != "" && properties["${system}.account".toString()] != "")) return

        BatchEuphoriaJobManager jobManager = createJobManagerFor(system)
        BEJob testJobWithPipedScript = new BEJob(null, "batchEuphoriaTestJob", null, testScript, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testJobWithPipedScript])
    }

    void checkAndPossiblyRunMultipleJobsWithPipedScript(ClusterScheduler system) {
        if (!(properties["${system}.host".toString()] != "" && properties["${system}.account".toString()] != "")) return

        BatchEuphoriaJobManager jobManager = createJobManagerFor(system)
        BEJob testParent = new BEJob(null, "batchEuphoriaTestJob_Parent", null, testScript, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild1 = new BEJob(null, "batchEuphoriaTestJob_Child1", null, testScript, null, resourceSet, [testParent], ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild2 = new BEJob(null, "batchEuphoriaTestJob_Child2", null, testScript, null, resourceSet, [testParent, testJobChild1], ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testParent, testJobChild1, testJobChild2])
    }

    void checkAndPossiblyRunJobWithFile(ClusterScheduler system) {
        if (!(properties["${system}.host".toString()] != "" && properties["${system}.account".toString()] != "" && properties["remoteToolPath"] != "")) return

        prepareTestScript(system)
        BatchEuphoriaJobManager jobManager = createJobManagerFor(system)
        BEJob testJobWithFile = new BEJob(null, "batchEuphoriaTestJob", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testJobWithFile])
    }

    void checkAndPossiblyRunMultipleJobsWithFile(ClusterScheduler system) {
        if (!(properties["${system}.host".toString()] != "" && properties["${system}.account".toString()] != "" && properties["remoteToolPath"] != "")) return

        prepareTestScript(system)
        BatchEuphoriaJobManager jobManager = createJobManagerFor(system)
        BEJob testParent = new BEJob(null, "batchEuphoriaTestJob_Parent", batchEuphoriaTestScript, null, null, resourceSet, null, ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild1 = new BEJob(null, "batchEuphoriaTestJob_Child1", batchEuphoriaTestScript, null, null, resourceSet, [testParent], ["a": "value"], jobManager, logFile, null)
        BEJob testJobChild2 = new BEJob(null, "batchEuphoriaTestJob_Child2", batchEuphoriaTestScript, null, null, resourceSet, [testParent, testJobChild1], ["a": "value"], jobManager, logFile, null)
        jobTest(jobManager, [testParent, testJobChild1, testJobChild2])
    }

    static void jobTest(BatchEuphoriaJobManager jobManager, List<BEJob> testJobs) {
        int maxSleep = 5

        testJobs.each {
            def jr = jobManager.submitJob(it)
            println "Job ID: ${jr.jobID.id}"
        }

        if (jobManager.isHoldJobsEnabled()) {

            ensureProperJobStates(maxSleep, testJobs, [JobState.HOLD], jobManager)

            jobManager.startHeldJobs(testJobs)
            // Wait for some seconds and see, if the status changes from HOLD to queued or running and from queued to running
            // The queued to running check can take a lot more time. Also the default update time for queries to the job system
            // is too long for tests. We force updates everytime we run queryJobStatus
            ensureProperJobStates(maxSleep, testJobs, [JobState.QUEUED, JobState.RUNNING], jobManager)
        } else {
            ensureProperJobStates(maxSleep, testJobs, [JobState.QUEUED, JobState.HOLD, JobState.RUNNING], jobManager)
        }

        jobManager.queryExtendedJobState(testJobs)

        ensureProperJobStates(maxSleep, testJobs, [JobState.COMPLETED_SUCCESSFUL], jobManager)

    }

    static void ensureProperJobStates(int maxSleep, List<BEJob> jobList, List<JobState> listOfStatesToCheck, BatchEuphoriaJobManager jobManager) {
        int increments = 8
        int sleep = maxSleep * increments  // 125ms increments, count from 5s to 0 seconds.
        boolean allJobsInCorrectState = false
        List<JobState> lastStates = []
        while (sleep > 0 && !allJobsInCorrectState) {
            lastStates.clear()
            def status = jobManager.queryJobStatus(jobList, true)
            allJobsInCorrectState = true
            for (BEJob job in jobList) {
                allJobsInCorrectState &= listOfStatesToCheck.contains(status[job])
                lastStates << status[job]
            }
            if (!allJobsInCorrectState) {
                assert status.values().join(" ").find(JobState.FAILED.name()) != JobState.FAILED.name()
                sleep--
            }
        }


        if (!allJobsInCorrectState && !lastStates.join(" ").find(JobState.RUNNING.name()))
            new BEException("Not all jobs ${jobList.collect { it.jobID }.join(" ")} were in the proper state: " +
                    "[${listOfStatesToCheck.join(" ")}], got last states [${lastStates.join(" ")}]. " +
                    "Make sure, that your job system is working properly.")
    }

    static synchronized void prepareTestScript(ClusterScheduler system) {
        if (testScriptWritten[system]) return

        batchEuphoriaTestScript = new File(properties."remoteToolPath" as String)
        BEExecutionService executionService = getExecutionServiceFor(system)
        executionService.execute("mkdir -p ${batchEuphoriaTestScript.parentFile}")
        if (properties."testscript" != "")
            testScript << properties."testscript"

        executionService.execute("echo ${testScript} > ${batchEuphoriaTestScript}")
        executionService.execute("chmod +x ${batchEuphoriaTestScript}")
        testScriptWritten[system] = true
    }

    @Test
    void testLsfMultipleJobsWithPipedScript() {
        checkAndPossiblyRunMultipleJobsWithPipedScript(ClusterScheduler.LSF)
    }

    @Test
    void testLsfMultipleJobsWithFile() {
        checkAndPossiblyRunMultipleJobsWithFile(ClusterScheduler.LSF)
    }

    @Test
    void testLsfJobWithFile() {
        checkAndPossiblyRunJobWithFile(ClusterScheduler.LSF)
    }

    @Test
    void testLsfJobWithPipedScript() {
        checkAndPossiblyRunJobWithPipedScript(ClusterScheduler.LSF)
    }

    /**
     * PBS
     */
    @Test
    void testPbsMultipleJobsWithPipedScript() {
        checkAndPossiblyRunMultipleJobsWithPipedScript(ClusterScheduler.PBS)
    }

    @Test
    void testPbsMultipleJobsWithFile() {
        checkAndPossiblyRunMultipleJobsWithFile(ClusterScheduler.PBS)
    }

    @Test
    void testPbsJobWithFile() {
        checkAndPossiblyRunJobWithFile(ClusterScheduler.PBS)
    }

    @Test
    void testPbsJobWithPipedScript() {
        checkAndPossiblyRunJobWithPipedScript(ClusterScheduler.PBS)
    }

    /**
     * Grid Engine
     */
    @Test
    void testSgeMultipleJobsWithPipedScript() {
        checkAndPossiblyRunMultipleJobsWithPipedScript(ClusterScheduler.SGE)
    }

    @Test
    void testSgeMultipleJobsWithFile() {
        checkAndPossiblyRunMultipleJobsWithFile(ClusterScheduler.SGE)
    }

    @Test
    void testSgeJobWithFile() {
        checkAndPossiblyRunJobWithFile(ClusterScheduler.SGE)
    }

    @Test
    void testSgeJobWithPipedScript() {
        checkAndPossiblyRunJobWithPipedScript(ClusterScheduler.SGE)
    }

    /**
     * Slurm
     */
    @Test
    void testSlurmMultipleJobsWithPipedScript() {
        checkAndPossiblyRunMultipleJobsWithPipedScript(ClusterScheduler.SLURM)
    }

    @Test
    void testSlurmMultipleJobsWithFile() {
        checkAndPossiblyRunMultipleJobsWithFile(ClusterScheduler.SLURM)
    }

    @Test
    void testSlurmJobWithFile() {
        checkAndPossiblyRunJobWithFile(ClusterScheduler.SLURM)
    }

    @Test
    void testSlurmJobWithPipedScript() {
        checkAndPossiblyRunJobWithPipedScript(ClusterScheduler.SLURM)
    }

    /**
     * LSF Rest is different, let's keep it separate
     */
    @Test
    void testLsfRestMultipleJobsWithPipedScript() {
        if ((properties["lsfrest.password"] as String).isEmpty()) return
        checkAndPossiblyRunMultipleJobsWithPipedScript(ClusterScheduler.LSF_REST)
    }

    @Test
    void testLsfRestJobWithPipedScript() {
        if ((properties["lsfrest.password"] as String).isEmpty()) return
        checkAndPossiblyRunJobWithPipedScript(ClusterScheduler.LSF_REST)
    }

    @Test
    void testLsfRestMultipleJobsWithFile() {
        if ((properties["lsfrest.password"] as String).isEmpty()) return
        checkAndPossiblyRunMultipleJobsWithFile(ClusterScheduler.LSF_REST)
    }

    @Test
    void testLsfRestJobWithFile() {
        if ((properties["lsfrest.password"] as String).isEmpty()) return
        checkAndPossiblyRunJobWithFile(ClusterScheduler.LSF_REST)
    }
}
