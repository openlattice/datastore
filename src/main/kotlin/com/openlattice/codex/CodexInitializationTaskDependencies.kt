package com.openlattice.codex

import com.openlattice.datastore.apps.services.AppService
import com.openlattice.tasks.HazelcastTaskDependencies

class CodexInitializationTaskDependencies(
        val appService: AppService
) : HazelcastTaskDependencies