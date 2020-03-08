# Changelog

## [0.9.0] - 2020-03-08
## Changed
- Replace the unstructured data file for storing key-value pairs with a write-ahead log.
### Added
- In the event of a crash or a power loss the database is automatically recovered.
- Optional background compaction allows reclaiming disk space occupied by overwritten or deleted keys.
### Fixed
- Fix disk space overhead when storing small keys and values. 

## [0.8.3] - 2019-11-03
### Fixed
- Fix slice bounds out of range error mapping files on Windows.

## [0.8.2] - 2019-09-04
### Fixed
- Race condition could lead to data corruption.

## [0.8.1] - 2019-06-30
### Fixed
- Fix panic when accessing closed database.
- Return error opening invalid database.

## [0.8] - 2019-03-30
### Changed
- ~2x write performance improvement on non-Windows.

## [0.7] - 2019-03-23
### Added
- Windows support (@mattn).
### Changed
- Improve freelist performance.

