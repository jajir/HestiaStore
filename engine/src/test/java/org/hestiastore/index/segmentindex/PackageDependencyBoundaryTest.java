package org.hestiastore.index.segmentindex;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.core.storage.StorageServiceBuilder;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Enforces SegmentIndex package dependency boundaries.
 */
@AnalyzeClasses(packages = "org.hestiastore.index", importOptions = ImportOption.DoNotIncludeTests.class)
class PackageDependencyBoundaryTest {

    private static final String SEGMENT_INDEX_PACKAGES = "org.hestiastore.index.segmentindex..";
    private static final String SEGMENT_REGISTRY_PACKAGES = "org.hestiastore.index.segmentregistry..";
    private static final String SEGMENT_INDEX_CORE_STORAGE_PACKAGES =
            "org.hestiastore.index.segmentindex.core.storage..";
    private static final String SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES =
            "org.hestiastore.index.segmentindex.core.topology..";
    private static final String SEGMENT_INDEX_CORE_SESSION_PACKAGES =
            "org.hestiastore.index.segmentindex.core.session..";
    private static final String SEGMENT_INDEX_CORE_SEGMENT_LEASE_PACKAGES =
            "org.hestiastore.index.segmentindex.core.segmentlease..";
    private static final String SEGMENT_INDEX_CORE_BOOTSTRAP_PACKAGES =
            "org.hestiastore.index.segmentindex.core.bootstrap..";
    private static final String SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES =
            "org.hestiastore.index.segmentindex.core.operations..";
    private static final String SEGMENT_INDEX_CORE_MAINTENANCE_PACKAGES =
            "org.hestiastore.index.segmentindex.core.maintenance..";
    private static final String SEGMENT_INDEX_MAINTENANCE_PACKAGES =
            "org.hestiastore.index.segmentindex.maintenance..";
    private static final String SEGMENT_INDEX_CORE_SPLIT_PACKAGES =
            "org.hestiastore.index.segmentindex.core.split..";
    private static final String SEGMENT_INDEX_CORE_STABLE_SEGMENT_PACKAGES =
            "org.hestiastore.index.segmentindex.core.stablesegment..";
    private static final String SEGMENT_INDEX_CORE_STREAMING_PACKAGES =
            "org.hestiastore.index.segmentindex.core.streaming..";
    private static final String SEGMENT_INDEX_CORE_EXECUTOR_REGISTRY_PACKAGES =
            "org.hestiastore.index.segmentindex.core.executorregistry..";
    private static final String SEGMENT_INDEX_METRICS_PACKAGES =
            "org.hestiastore.index.segmentindex.metrics..";
    private static final String SEGMENT_INDEX_CORE_PACKAGES =
            "org.hestiastore.index.segmentindex.core..";
    private static final String SEGMENT_INDEX_MAPPING_PACKAGES =
            "org.hestiastore.index.segmentindex.mapping..";
    private static final String SEGMENT_INDEX_EFFECTIVE_CONFIGURATION_PACKAGES =
            "org.hestiastore.index.segmentindex.configuration.effective..";
    private static final String SEGMENT_INDEX_TUNING_CONFIGURATION_PACKAGES =
            "org.hestiastore.index.segmentindex.configuration.tuning..";
    private static final String REMOVED_SEGMENT_INDEX_CONFIG_PACKAGES =
            "org.hestiastore.index.segmentindex.config..";
    private static final String REMOVED_SEGMENT_INDEX_RUNTIME_CONFIG_PACKAGES =
            "org.hestiastore.index.segmentindex.runtimeconfiguration..";
    private static final String REMOVED_SEGMENT_INDEX_CORE_CONTROL_PACKAGES =
            "org.hestiastore.index.segmentindex.core.control..";
    private static final String SEGMENT_INDEX_PUBLIC_API_PACKAGE =
            "org.hestiastore.index.segmentindex";
    private static final String STORAGE_CONSISTENCY_CHECKER_CLASS =
            "org.hestiastore.index.segmentindex.core.storage.IndexConsistencyChecker";
    private static final String STORAGE_CONSISTENCY_COORDINATOR_CLASS =
            "org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator";
    private static final String STORAGE_CONSISTENCY_INTERNAL_CLASS_PATTERN =
            ".*\\.core\\.storage\\.IndexConsistency(Checker|Coordinator)";
    private static final String STORAGE_WAL_COORDINATOR_CLASS =
            "org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator";

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
                    SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES,
                    SEGMENT_INDEX_CORE_SEGMENT_LEASE_PACKAGES,
                    SEGMENT_INDEX_CORE_SPLIT_PACKAGES,
                    SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES,
                    SEGMENT_INDEX_CORE_MAINTENANCE_PACKAGES,
                    SEGMENT_INDEX_CORE_STREAMING_PACKAGES,
                    SEGMENT_INDEX_CORE_STABLE_SEGMENT_PACKAGES,
                    SEGMENT_INDEX_METRICS_PACKAGES)
            .because("storage owns persisted state, WAL, recovery, and consistency work; it must not depend on runtime orchestration packages.");

    @ArchTest
    static final ArchRule segment_index_topology_does_not_depend_on_session = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SESSION_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_topology_does_not_depend_on_segment_lease = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SEGMENT_LEASE_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_topology_does_not_depend_on_physical_storage_or_split = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_STORAGE_PACKAGES,
                    SEGMENT_INDEX_CORE_SPLIT_PACKAGES,
                    SEGMENT_REGISTRY_PACKAGES)
            .because("topology owns route state only; physical segment storage, recovery cleanup, and split orchestration belong elsewhere.");

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
    static final ArchRule bootstrap_does_not_depend_on_segment_index_public_api_package = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_BOOTSTRAP_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAPackage(SEGMENT_INDEX_PUBLIC_API_PACKAGE)
            .because("bootstrap composes runtime internals and must not reach through the public SegmentIndex API package for helper objects.");

    @ArchTest
    static final ArchRule concrete_retry_policies_are_package_private = classes()//
            .that()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_PACKAGES,
                    SEGMENT_REGISTRY_PACKAGES)//
            .and()//
            .haveSimpleNameEndingWith("RetryPolicy")//
            .should()//
            .bePackagePrivate()
            .because("retry policies are package-local implementation details; dependent packages pass retry settings to the package entry point instead of sharing policy objects.");

    @ArchTest
    static final ArchRule mapping_does_not_depend_on_split = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_MAPPING_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_SPLIT_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_split_does_not_depend_on_topology = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_SPLIT_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES);

    @ArchTest
    static final ArchRule segment_index_split_does_not_depend_on_storage = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_SPLIT_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)
            .because("split may materialize and delete segments through the registry, but map-to-directory recovery belongs to StorageService.");

    @ArchTest
    static final ArchRule segment_index_operations_do_not_bypass_route_state = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_TOPOLOGY_PACKAGES,
                    SEGMENT_INDEX_MAPPING_PACKAGES)
            .because("foreground operations should resolve route state through SegmentLeaseService, not by combining route maps and topology directly.");

    @ArchTest
    static final ArchRule segment_index_operations_do_not_load_segments_directly_from_registry = noClasses()//
            .that()//
            .resideInAPackage(SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .areAssignableTo(SegmentRegistry.class)
            .because("foreground operations may use the leased BlockingSegment, but segment loading must stay behind SegmentLeaseService.");

    @ArchTest
    static final ArchRule only_storage_session_bootstrap_operations_and_maintenance_use_storage_service = noClasses()//
            .that()//
            .resideOutsideOfPackages(SEGMENT_INDEX_CORE_STORAGE_PACKAGES,
                    SEGMENT_INDEX_CORE_SESSION_PACKAGES,
                    SEGMENT_INDEX_CORE_BOOTSTRAP_PACKAGES,
                    SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES,
                    SEGMENT_INDEX_MAINTENANCE_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .areAssignableTo(StorageService.class)
            .because("StorageService is the storage boundary used by controlled runtime, bootstrap, operation, and maintenance entry points; broad direct use would turn it into a service locator.");

    @ArchTest
    static final ArchRule only_storage_and_bootstrap_build_storage_service = noClasses()//
            .that()//
            .resideOutsideOfPackages(SEGMENT_INDEX_CORE_STORAGE_PACKAGES,
                    SEGMENT_INDEX_CORE_BOOTSTRAP_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .areAssignableTo(StorageServiceBuilder.class)
            .because("bootstrap owns startup composition and storage owns storage internals; other packages should use StorageService instead of constructing storage services directly.");

    @ArchTest
    static final ArchRule storage_consistency_internals_are_package_private = classes()//
            .that()//
            .haveFullyQualifiedName(STORAGE_CONSISTENCY_CHECKER_CLASS)//
            .or()//
            .haveFullyQualifiedName(STORAGE_CONSISTENCY_COORDINATOR_CLASS)//
            .should()//
            .bePackagePrivate()
            .because("consistency checking couples the key-to-segment map with physical segments, so callers outside storage must use StorageService.");

    @ArchTest
    static final ArchRule only_storage_package_uses_storage_consistency_internals = noClasses()//
            .that()//
            .resideOutsideOfPackages(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .haveNameMatching(STORAGE_CONSISTENCY_INTERNAL_CLASS_PATTERN)
            .because("StorageService is the package entry point for consistency checks; external code must not wire checker/coordinator internals directly.");

    @ArchTest
    static final ArchRule storage_wal_coordinator_is_package_private = classes()//
            .that()//
            .haveFullyQualifiedName(STORAGE_WAL_COORDINATOR_CLASS)//
            .should()//
            .bePackagePrivate()
            .because("WAL replay, append, checkpoint, and applied-LSN tracking belong behind StorageService.");

    @ArchTest
    static final ArchRule only_storage_package_uses_wal_coordinator = noClasses()//
            .that()//
            .resideOutsideOfPackages(SEGMENT_INDEX_CORE_STORAGE_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .haveFullyQualifiedName(STORAGE_WAL_COORDINATOR_CLASS)
            .because("StorageService is the package entry point for WAL coordination; external code must not depend on IndexWalCoordinator directly.");

    @ArchTest
    static final ArchRule operational_runtime_packages_do_not_depend_on_metrics = noClasses()//
            .that()//
            .resideInAnyPackage(SEGMENT_INDEX_CORE_OPERATIONS_PACKAGES,
                    SEGMENT_INDEX_CORE_MAINTENANCE_PACKAGES,
                    SEGMENT_INDEX_CORE_SPLIT_PACKAGES,
                    SEGMENT_INDEX_CORE_EXECUTOR_REGISTRY_PACKAGES)//
            .should()//
            .dependOnClassesThat()//
            .resideInAnyPackage(SEGMENT_INDEX_METRICS_PACKAGES);
}
