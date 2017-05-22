/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution.cluster.pbs

import de.dkfz.eilslabs.batcheuphoria.config.ResourceSet
import de.dkfz.eilslabs.batcheuphoria.config.ResourceSetSize
import de.dkfz.eilslabs.batcheuphoria.jobs.Job
import de.dkfz.roddy.tools.BufferUnit
import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic
import org.junit.Test

import java.io.File

import static org.junit.Assert.*

/**
 * Created by heinold on 26.03.17.
 */
@CompileStatic
class PBSCommandTest {
    @Test
    void testToString() throws Exception {

    }

    @Test
    void testAssembleVariableExportString() throws Exception {
        def mapOfParameters = ["a": "a", "b": "b"]
        Job job = new Job("Test", new File("/tmp/test.sh"),null, null, new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 4, 1, new TimeUnit("1h"), null, null, null), null, mapOfParameters, null, null, null)
        PBSCommand cmd = new PBSCommand(null, job, "id", null, mapOfParameters, null, null, null, "/tmp/test.sh", null)
        String result = cmd.assembleVariableExportString()
        assert result == " -v PARAMETER_FILE=" + job.parameterFile
    }

    @Test
    void testAssembleDependencyStringWithoutDependencies() throws Exception {
        def mapOfParameters = ["a": "a", "b": "b"]
        Job job = new Job("Test", new File("/tmp/test.sh"),null, null, new ResourceSet(ResourceSetSize.l, new BufferValue(1, BufferUnit.G), 4, 1, new TimeUnit("1h"), null, null, null), null, mapOfParameters, null, null, null)
        PBSCommand cmd = new PBSCommand(null, job, "id", null, mapOfParameters, null, null, null, "/tmp/test.sh", null)
        String result = cmd.assembleDependencyString()
        assert result == ""
    }

}