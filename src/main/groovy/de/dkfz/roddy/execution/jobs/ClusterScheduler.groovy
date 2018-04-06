/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution.jobs

import de.dkfz.roddy.execution.jobs.cluster.lsf.LSFJobManager
import de.dkfz.roddy.execution.jobs.cluster.lsf.rest.LSFRestJobManager
import de.dkfz.roddy.execution.jobs.cluster.pbs.PBSJobManager
import de.dkfz.roddy.execution.jobs.cluster.sge.SGEJobManager
import de.dkfz.roddy.execution.jobs.direct.synchronousexecution.DirectSynchronousExecutionJobManager
import groovy.transform.CompileStatic
import groovy.transform.PackageScope

/**
 * A list of available cluster systems.
 * testable means that the tests are basically implemented
 * Created by heinold on 27.03.17.
 */
@CompileStatic
enum ClusterScheduler {
    DIRECT(DirectSynchronousExecutionJobManager),
    PBS(PBSJobManager),
    SGE(SGEJobManager),
    SLURM(SGEJobManager),
    LSF(LSFJobManager),
    LSF_REST(LSFRestJobManager)

    @PackageScope final String className

    ClusterScheduler(Class<? extends BatchEuphoriaJobManager> cls) {
        this.className = cls.name
    }
}
