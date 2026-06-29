package org.hestiastore.index.segmentindex;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Enforces SegmentIndex package dependency boundaries.
 */
@AnalyzeClasses(packages = "org.hestiastore.index", importOptions = ImportOption.DoNotIncludeTests.class)
class PackageDependencyBoundaryTest {

    private static final String SEGMENT_INDEX_PACKAGES = "org.hestiastore.index.segmentindex..";
    private static final String SEGMENT_REGISTRY_PACKAGES = "org.hestiastore.index.segmentregistry..";
    private static final String SEGMENT_INDEX_CORE_STORAGE_PACKAGES = "org.hestiastore.index.segmentindex.core.storage..";
    private static final String SEGMENT_INDEX_CORE_ROUTING_PACKAGES = "org.hestiastore.index.segmentindex.core.routing..";
    private static final String SEGMENT_INDEX_CORE_SESSION_PACKAGES = "org.hestiastore.index.segmentindex.core.session..";
    private static final String SEGMENT_INDEX_CORE_EXECUTION_PACKAGES = "org.hestiastore.index.segmentindex.core.execution..";
    private static final String SEGMENT_INDEX_CORE_SPLIT_PACKAGES = "org.hestiastore.index.segmentindex.core.split..";
    private static final String SEGMENT_INDEX_CORE_EXECUTOR_REGISTRY_PACKAGES = "org.hestiastore.index.segmentindex.core.executorregistry..";
    private static final String SEGMENT_INDEX_RUNTIME_MONITORING_PACKAGES = "org.hestiastore.index.segmentindex.monitoring..";
    private static final String SEGMENT_INDEX_ROUTE_MAP_PACKAGES = "org.hestiastore.index.segmentindex.routemap..";
    private static final String SEGMENT_INDEX_EFFECTIVE_CONFIGURATION_PACKAGES = "org.hestiastore.index.segmentindex.configuration.effective..";
    private static final String SEGMENT_INDEX_TUNING_CONFIGURATION_PACKAGES = "org.hestiastore.index.segmentindex.configuration.tuning..";
    private static final String REMOVED_SEGMENT_INDEX_CONFIG_PACKAGES = "org.hestiastore.index.segmentindex.config..";
    private static final String REMOVED_SEGMENT_INDEX_RUNTIME_CONFIG_PACKAGES = "org.hestiastore.index.segmentindex.runtimeconfiguration..";
    private static final String REMOVED_SEGMENT_INDEX_CORE_CONTROL_PACKAGES = "org.hestiastore.index.segmentindex.core.control..";
    private static final String SEGMENT_INDEX_PUBLIC_API_PACKAGE = "org.hestiastore.index.segmentindex";
    private static final String SEGMENT_INDEX_BOOTSTRAP_OPERATION_CLASS = "org.hestiastore.index.segmentindex.core.session.SegmentIndexBootstrapOperation";
    private static final String STORAGE_CONSISTENCY_CHECKER_CLASS = "org.hestiastore.index.segmentindex.core.storage.RouteMapConsistencyChecker";
    private static final String STORAGE_CONSISTENCY_COORDINATOR_CLASS = "org.hestiastore.index.segmentindex.core.storage.StorageConsistencyCoordinator";
    private static final String STORAGE_CONSISTENCY_INTERNAL_CLASS_PATTERN = ".*\\.core\\.storage\\.(RouteMapConsistencyChecker|StorageConsistencyCoordinator)";

    @ArchTest
    static final ArchRule only_segment_index_packages_depend_on_segment_registry = noClasses()//
            .that() //
            .resideOutsideOfPackages(SEGMENT_INDEX_PACKAGES,
                    SEGMENT_REGISTRY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_REGISTRY_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_storage_does_not_depend_on_session = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SESSION_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_storage_does_not_depend_on_runtime_orchestration_packages = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(
                    SEGMENT_INDEX_CORE_ROUTING_PACKAGES,
                    SEGMENT_INDEX_CORE_SPLIT_PACKAGES,
                    SEGMENT_INDEX_CORE_EXECUTION_PACKAGES,
                    SEGMENT_INDEX_RUNTIME_MONITORING_PACKAGES)
            .because(
                    "storage owns persisted state, WAL, recovery, and consistency work; it must not depend on runtime orchestration packages.");

    @ArchTest
    static final ArchRule segment_index_routing_does_not_depend_on_session = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_ROUTING_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SESSION_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_routing_does_not_depend_on_physical_storage_or_split = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_ROUTING_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_STORAGE_PACKAGES,
                    SEGMENT_INDEX_CORE_SPLIT_PACKAGES)
            .because(
                    "routing owns route state and segment leases; physical storage, recovery cleanup, and split orchestration belong elsewhere.");

    @ArchTest
    static final ArchRule removed_segment_index_packages_are_not_used_by_internal_code = noClasses()//
            .that()//
            .resideOutsideOfPackages(REMOVED_SEGMENT_INDEX_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_RUNTIME_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_CORE_CONTROL_PACKAGES,
                    SEGMENT_INDEX_PUBLIC_API_PACKAGE)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(REMOVED_SEGMENT_INDEX_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_RUNTIME_CONFIG_PACKAGES,
                    REMOVED_SEGMENT_INDEX_CORE_CONTROL_PACKAGES);

    @ArchTest
    static final ArchRule effective_configuration_does_not_depend_on_runtime_tuning = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_EFFECTIVE_CONFIGURATION_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_TUNING_CONFIGURATION_PACKAGES);

    @ArchTest
    static final ArchRule route_map_does_not_depend_on_split = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_ROUTE_MAP_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SPLIT_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_split_does_not_depend_on_storage = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_SPLIT_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)
            .because(
                    "split may materialize and delete segments through the registry, but map-to-directory recovery belongs to StorageCoordinator.");

    @ArchTest
    static final ArchRule only_storage_session_operations_and_maintenance_use_storage_service = noClasses()//
            .that()//
            .resideOutsideOfPackages(SEGMENT_INDEX_CORE_STORAGE_PACKAGES,
                    SEGMENT_INDEX_CORE_SESSION_PACKAGES,
                    SEGMENT_INDEX_CORE_EXECUTION_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .areAssignableTo(StorageCoordinator.class)
            .because(
                    "StorageCoordinator is the storage boundary used by controlled runtime, session startup, operation, and maintenance entry points; broad direct use would turn it into a service locator.");

    @ArchTest
    static final ArchRule only_storage_and_session_startup_create_storage_service = noClasses()//
            .that()//
            .resideOutsideOfPackages(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)//
            .and()//
            .doNotHaveFullyQualifiedName(
                    SEGMENT_INDEX_BOOTSTRAP_OPERATION_CLASS)//
            .should()//
            .callMethod(StorageCoordinator.class, "create", Directory.class,
                    SegmentRouteMap.class, SegmentRegistry.class,
                    EffectiveIndexMaintenanceConfiguration.class)
            .because(
                    "session startup owns runtime composition and storage owns storage internals; other packages should use the already-created StorageCoordinator.");

    @ArchTest
    static final ArchRule storage_consistency_internals_are_package_private = classes()//
            .that()//
            .haveFullyQualifiedName(STORAGE_CONSISTENCY_CHECKER_CLASS)//
            .or()//
            .haveFullyQualifiedName(STORAGE_CONSISTENCY_COORDINATOR_CLASS)//
            .should()//
            .bePackagePrivate()
            .because(
                    "consistency checking couples the key-to-segment map with physical segments, so callers outside storage must use StorageCoordinator.");

    @ArchTest
    static final ArchRule only_storage_package_uses_storage_consistency_internals = noClasses()//
            .that()//
            .resideOutsideOfPackages(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .haveNameMatching(STORAGE_CONSISTENCY_INTERNAL_CLASS_PATTERN)
            .because(
                    "StorageCoordinator is the package entry point for consistency checks; external code must not wire checker/coordinator internals directly.");

    @ArchTest
    static final ArchRule operational_runtime_packages_do_not_depend_on_metrics = noClasses()//
            .that()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_EXECUTION_PACKAGES,
                    SEGMENT_INDEX_CORE_SPLIT_PACKAGES,
                    SEGMENT_INDEX_CORE_EXECUTOR_REGISTRY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_RUNTIME_MONITORING_PACKAGES);

    @ArchTest
    static final ArchRule only_monitoring_api_and_runtime_exposure_depend_on_runtime_monitoring = noClasses()//
            .that()//
            .resideOutsideOfPackages(
                    SEGMENT_INDEX_RUNTIME_MONITORING_PACKAGES,
                    SEGMENT_INDEX_PUBLIC_API_PACKAGE,
                    SEGMENT_INDEX_CORE_SESSION_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_RUNTIME_MONITORING_PACKAGES)
            .because(
                    "runtime monitoring should stay behind the public SegmentIndex API, monitoring infrastructure, bootstrap composition, and session-level API exposure.");
}
