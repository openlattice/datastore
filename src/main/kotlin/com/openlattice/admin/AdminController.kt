package com.openlattice.admin

import com.hazelcast.core.HazelcastInstance
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principal
import com.openlattice.authorization.Principals
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.PrincipalSet
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(com.openlattice.auditing.CONTROLLER)
class AdminController : AdminApi, AuthorizingComponent {

    @Inject
    private lateinit var auditRecordEntitySetsManager: AuditRecordEntitySetsManager

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Inject
    private lateinit var hazelcast: HazelcastInstance


    @GetMapping(value = [RELOAD_CACHE])
    override fun reloadCache() {
        ensureAdminAccess()
        HazelcastMap.values().forEach { hazelcast.getMap<Any, Any>(it.name).loadAll(true) }
    }

    @GetMapping(value = [RELOAD_CACHE + NAME_PATH])
    override fun reloadCache(name: String) {
        ensureAdminAccess()
        hazelcast.getMap<Any, Any>(HazelcastMap.valueOf(name).name).loadAll(true)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

    @GetMapping(value = [PRINCIPALS + ID_PATH])
    override fun getUserPrincipals(principalId: String): Set<Principal> {
        ensureAdminAccess()
        return Principals.getUserPrincipals(principalId)
    }
}