# SegmentRegistry Audit

Date: 2026-02-13  
Scope: `src/main/java/org/hestiastore/index/segmentregistry`

## M41 Findings (unused/test-only)

- Removed unused field:
  - `SegmentRegistryImpl#directoryFacade` (stored but never read).
- Removed test-only production helper:
  - `SegmentRegistryAccessImpl#forHandler(status, handler)` overload.
  - Tests now use `forHandler(status, handler, cache, segmentId)` with
    `null` pin arguments when pinning is not needed.
- Tightened visibility:
  - `SegmentHandler#getState()` changed from `public` to package-private.
- Compatibility note (identified, intentionally kept):
  - `SegmentRegistryResultStatus.NOT_FOUND` and
    `SegmentRegistryResult.notFound()` are currently used only by tests in this
    repository, but remain as legacy compatibility API and are documented as
    such.

## M42 Findings (tests + Javadocs)

- Public Javadocs check: no missing Javadocs found for public classes/methods
  in `segmentregistry`.
- Test coverage matrix:
  - `DirectorySegmentIdAllocator` -> `DirectorySegmentIdAllocatorTest`
  - `SegmentFactory` -> `SegmentFactoryTest`
  - `SegmentHandler` -> `SegmentHandlerTest`
  - `SegmentHandlerLockStatus` -> `SegmentHandlerLockStatusTest`
  - `SegmentHandlerState` -> `SegmentHandlerStateTest`
  - `SegmentRegistryAccessImpl` -> `SegmentRegistryAccessImplTest`
  - `SegmentRegistryBuilder` -> `SegmentRegistryBuilderTest`
  - `SegmentRegistryCache` -> `SegmentRegistryCacheTest`
  - `SegmentRegistryFileSystem` -> `SegmentRegistryFileSystemTest`
  - `SegmentRegistryImpl` -> `SegmentRegistryImplTest`
  - `SegmentRegistryResult` -> `SegmentRegistryResultTest`
  - `SegmentRegistryResultStatus` -> `SegmentRegistryResultStatusTest`
  - `SegmentRegistryState` -> `SegmentRegistryStateTest`
  - `SegmentRegistryStateMachine` -> `SegmentRegistryStateMachineTest`
- Interface coverage (no standalone class behavior to instantiate):
  - `SegmentRegistry` covered by `SegmentRegistryImplTest` and
    `SegmentRegistryTest`.
  - `SegmentRegistryAccess` covered by `SegmentRegistryAccessImplTest` and
    `SegmentRegistryTest`.
  - `SegmentIdAllocator` covered by `DirectorySegmentIdAllocatorTest` and
    `SegmentRegistryBuilderTest`.

## Verification Run

- `mvn -q -Dtest=DirectorySegmentIdAllocatorTest,SegmentFactoryTest,SegmentHandlerTest,SegmentHandlerLockStatusTest,SegmentHandlerStateTest,SegmentRegistryAccessImplTest,SegmentRegistryBuilderTest,SegmentRegistryCacheTest,SegmentRegistryFileSystemTest,SegmentRegistryStateMachineTest,SegmentRegistryImplTest,SegmentRegistryResultTest,SegmentRegistryResultStatusTest,SegmentRegistryStateTest,SegmentRegistryTest test`
