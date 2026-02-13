# SegmentRegistry Audit

Date: 2026-02-13  
Scope: `src/main/java/org/hestiastore/index/segmentregistry`

## M41 Findings (unused/test-only)

- Removed unused production API from `SegmentHandler`:
  - removed `getSegmentIfReady()`
  - removed `isForSegment(...)`
- Tightened internal visibility:
  - `SegmentRegistryAccessImpl#forHandler(status, handler)` is now package-private.
- Removed dead code:
  - removed unused `SegmentRegistryAccessImpl#forHandlerValue(...)`.
- Kept public visibility for extension points used across packages:
  - `SegmentRegistry`, `SegmentRegistryBuilder`, `SegmentIdAllocator`, `SegmentFactory`.

## M42 Findings (tests + Javadocs)

- Added missing dedicated tests:
  - `SegmentRegistryAccessImplTest`
  - `SegmentRegistryStateMachineTest`
- Added missing Javadocs:
  - `SegmentRegistryAccessImpl` public factory/accessor methods
  - `DirectorySegmentIdAllocator#nextId()` (`{@inheritDoc}`)
- Coverage result:
  - Every concrete class in `segmentregistry` has a dedicated JUnit test class.
  - Interfaces/enums are covered indirectly by implementation/behavior tests.
