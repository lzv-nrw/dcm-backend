# Changelog

## [3.0.0] - 2025-07-25

### Changed

- **Breaking:** refactored ingest-API and related components
- **Breaking:** switched to template-based jobs
- **Breaking:** refactored job-API
- **Breaking:** refactored `Scheduler` component and related components (added support for scheduled onetime-execution, monthly-execution, time-specific execution)
- refactored and extended app-extensions
- **Breaking:** replaced key-value store-database with sql-database
- **Breaking:** limited incremental updates for user configurations to only secrets
- **Breaking:** replaced roles with groups-object in UserConfig-model

### Added

- added creation/update-metadata to UserConfig-model
- added support for Preparation Module in the dcm-pipeline
- added database-related app-extensions
- added generation of extended demo-data
- added new template-API endpoints
- added new workspace-API endpoints
- added *demo* administrator-account

### Fixed

- fixed orchestrator initialization (missing `nprocesses`-arg)
- fixed data-model of UserConfig (broken `dataclass`-decorator)
- fixed handling of termination signals in scheduler

## [2.0.0] - 2025-02-17

### Changed

- **Breaking:** changed url paths for the job configuration endpoints in API v1

### Added

- added new user-API endpoints from API v1
- added new configuration-API endpoints from API v1

## [1.0.2] - 2024-11-21

### Changed

- updated package metadata, Dockerfiles, and README

## [1.0.1] - 2024-10-22

### Fixed

- added scheduling initialization based on existing configurations

## [1.0.0] - 2024-10-16

### Added

- implemented backend-API v0 (`8569c869`, `088bcbe8`, `43fc8277`, `b879094d`, `ed44e915`)
- added Job Processor adapter (`cab9f850`)
- completed project template (`54d3d19b`, `824c7f9a`)
- added initial app configuration (`857a3807`, `824c7f9a`)

## [0.2.0] - 2024-07-26

### Changed

- improved error handling in the `ArchiveController` by logging parsed response (`c190d058`)

### Fixed

- fixed log-message in the `ArchiveController` in case of 204-response to a GET-request (`35db08f9`)

## [0.1.0] - 2024-07-25

### Changed

- initial release of the dcm-backend
