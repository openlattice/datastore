package com.openlattice.codex

import com.hazelcast.core.HazelcastInstance
import com.openlattice.apps.App
import com.openlattice.apps.AppType
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.collections.LinkedHashSet

// NOTE: once the apps-v2 changes go in, this will all go away
val APP_TYPES = listOf(
        AppType(FullQualifiedName("app.contactinfogiven"), "Contact Info Given", Optional.empty(), UUID.fromString("cf9b4d36-6f1a-4e6f-94f0-6458054fc567")),
        AppType(FullQualifiedName("app.sentfrom"), "Sent From", Optional.empty(), UUID.fromString("cf9b4d36-6f1a-4e6f-94f0-6458054fc567")),
        AppType(FullQualifiedName("app.sentto"), "Sent To", Optional.empty(), UUID.fromString("766e8284-fb5d-4d38-9595-99b6bca99a3b")),
        AppType(FullQualifiedName("app.contactinformation"), "Contact Information", Optional.empty(), UUID.fromString("2e20c21e-6448-4a2d-bc57-9a5f2c45b589")),
        AppType(FullQualifiedName("app.settings"), "Settings", Optional.empty(), UUID.fromString("bc0f0785-6af4-4a16-ab01-ef125d8fa183")),
        AppType(FullQualifiedName("app.subjectof"), "Subject Of", Optional.empty(), UUID.fromString("34aeaca4-d424-43cd-a3b4-f2032d583280")),
        AppType(FullQualifiedName("app.people"), "People", Optional.empty(), UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab")),
        AppType(FullQualifiedName("app.messages"), "Messages", Optional.empty(), UUID.fromString("8d0e38de-4302-4ec2-a1a7-f9159086e60e"))
)

class CodexInitializationTask : HazelcastInitializationTask<CodexInitializationTaskDependencies> {
    override fun getInitialDelay(): Long {
        return 0
    }

    companion object {
        val logger = LoggerFactory.getLogger(CodexInitializationTask::class.java)
    }

    override fun initialize(dependencies: CodexInitializationTaskDependencies) {

        val appTypeIds = LinkedHashSet(APP_TYPES.map {
            try {
                dependencies.appService.getAppType(it.type).id
            } catch (e: Exception) {
                dependencies.appService.createAppType(it)
            }
        })

        try {
            dependencies.appService.createApp(App(
                    "codex",
                    "Codex",
                    Optional.empty(),
                    appTypeIds,
                    ""
            ))
        } catch (e: Exception) {
            logger.info("Skipping creation of codex app.")
        }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf()
    }

    override fun getName(): String {
        return Task.CODEX_INITIALIZER.name
    }

    override fun getDependenciesClass(): Class<out CodexInitializationTaskDependencies> {
        return CodexInitializationTaskDependencies::class.java
    }

}