# SegmentRegistry Audit

Date: 2026-02-13  
Scope: `src/main/java/org/hestiastore/index/segmentregistry`

## M41 Findings (unused/test-only)

- Removed unused field:
  - `SegmentRegistryImpl#directoryFacade` (stored but never read).
- Removed test-only production helper:
  - removed cache-pin based `SegmentRegistryAccessImpl#forHandler(...)` shape.
  - `SegmentRegistryAccessImpl` now exposes a single handler path:
    `forHandler(status, handler)`.
- Tightened visibility:
  - `SegmentHandler#getState()` changed from `public` to package-private.
- Removed legacy status code path that was outside `registry.md` contract:
  - removed `SegmentRegistryResultStatus.NOT_FOUND`
  - removed `SegmentRegistryResult.notFound()`
- Removed reference-pin/cache-retain path from registry cache entry lifecycle:
  - removed `retain/release` APIs
  - removed `Entry.refCount`
  - unload eligibility now relies on per-entry state and value predicate,
    not pin counting.

## M42 Findings (tests + Javadocs)

- Public Javadocs check: no missing Javadocs found for public classes/methods
  in `segmentregistry`.
- Contract alignment updates to match `registry.md`:
  - missing segment load in established registry now throws runtime exception
    (exception-driven policy).
  - removed implicit create-on-miss from `getSegment`; segment creation is only
    via `createSegment()`.
  - `Entry.tryStartUnload()` now returns boolean and value retrieval is explicit
    (`getValueForUnload()`), matching diagram/state-machine intent.
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

- `mvn -q -Dtest=DirectorySegmentIdAllocatorTest,SegmentFactoryTest,SegmentHandlerTest,SegmentHandlerLockStatusTest,SegmentHandlerStateTest,SegmentRegistryAccessImplTest,SegmentRegistryBuilderTest,SegmentRegistryCacheTest,SegmentRegistryFileSystemTest,SegmentRegistryStateMachineTest,SegmentRegistryImplTest,SegmentRegistryResultStatusTest,SegmentRegistryStateTest,SegmentRegistryTest test`
