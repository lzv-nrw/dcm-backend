# Digital Curation Manager - Backend

The 'DCM Backend'-API provides functionality to
* trigger an ingest in the archive-system,
* collect the current ingest-status,
* control job execution,
* download job-artifacts,
* cleanup job-artifacts,
* authenticate users, and
* manage user/workspace/template/job configurations.

This repository contains the corresponding Flask app definition.
For the associated OpenAPI-document, please refer to the sibling package [`dcm-backend-api`](https://github.com/lzv-nrw/dcm-backend-api).

The contents of this repository are part of the [`Digital Curation Manager`](https://github.com/lzv-nrw/digital-curation-manager).

## Ex Libris Rosetta authorization HTTP header

Using the Rosetta REST-API requires authentication via the `Basic` HTTP authentication scheme (see [docs](https://developers.exlibrisgroup.com/rosetta/apis/rest-apis/)).
The file `convert_credentials.sh` contains a helper script to generate that headers contents.
An output destination can optionally be given as first argument (with default being `~/.rosetta/rosetta_auth`).
The user is then prompted to provide the institution code, the username, and the password for the specific Rosetta instance.

In order to test the generated header, use, for example, the following curl-command:
```
curl -X GET "<API-base-url>/rest/v0/deposits" -H @<path-to-header-file> -H 'accept: application/json'
```

## Local install
Make sure to include the extra-index-url `https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple` in your [pip-configuration](https://pip.pypa.io/en/stable/cli/pip_install/#finding-packages) to enable an automated install of all dependencies.
Using a virtual environment is recommended.

1. Install with
   ```
   pip install .
   ```
1. Configure service environment to fit your needs ([see here](#environmentconfiguration)).
1. Run app as
   ```
   flask run --port=8080
   ```
1. To manually use the API, either run command line tools like `curl` as, e.g.,
   ```
   curl -X 'POST' \
     'http://localhost:8080/ingest' \
     -H 'accept: application/json' \
     -H 'Content-Type: application/json' \
     -d '{
     "ingest": {
       "archiveId": "40be1c3a-2a6a-4656-9996-078fc9364ac4",
       "target": {
         "subdirectory": "2468edf8-6706-4ff0-bd03-04512d082c28"
       }
     }
   }'
   ```
   or run a gui-application, like Swagger UI, based on the OpenAPI-document provided in the sibling package [`dcm-backend-api`](https://github.com/lzv-nrw/dcm-backend-api).

## Docker
Build an image using, for example,
```
docker build -t dcm/backend:dev .
```
Then run with
```
docker run --rm --name=backend -v ~/.rosetta/rosetta_auth:/home/dcm/.rosetta/rosetta_auth -p 8080:80 dcm/backend:dev
```
and test by making a GET-http://localhost:8080/identify request.

For additional information, refer to the documentation [here](https://github.com/lzv-nrw/digital-curation-manager).

## Tests
Install additional dev-dependencies with
```
pip install -r dev-requirements.txt
```
Run unit-tests with
```
pytest -v -s
```

## Environment/Configuration
Service-specific environment variables are

### Artifact cleanup
The backend-service can be used to automatically clean up the shared file-storage.
This is implemented by periodically checking for new contents in the targeted directories.
Once a new object is detected, it is assigned an expiration-datetime (stored in the database).
If an object exceeds this expiration-datetime, it is deleted during the next scheduled run of the cleanup-routine.
The following variables can be used to configure that procedure
* `CLEANUP_DISABLED` [DEFAULT 0]: whether to disable the automated cleanup
* `CLEANUP_TARGETS` [DEFAULT '["ie", "ip", "pip", "sip"]']: targeted directories with automated cleanup as a JSON-array of strings; relative to shared file storage
* `CLEANUP_INTERVAL` [DEFAULT 3600]: interval of the cleanup-routine in seconds
* `CLEANUP_ARTIFACT_TTL` [DEFAULT 604800 (7 days)]: duration until an artifact expires (after detection) in seconds

### Database
* `DB_LOAD_SCHEMA` [DEFAULT 0]: whether the database should be initialized with the database schema
* `DB_GENERATE_DEMO` [DEFAULT 0]: whether database-tables and related configuration should be filled with demo-data at startup

  This includes an administrator account.
  The randomized password is printed to stdout after data has been generated (see also `DB_DEMO_ADMIN_PW`).
* `DB_GENERATE_DEMO_USERS` [DEFAULT 1]: whether additional demo users are created at startup (only applies if `DB_GENERATE_DEMO` is set)

  This includes three regular users.
  User 'einstein' with password 'relativity', user 'curie' with password 'radioactivity', and user 'feynman' with password 'superfluidity'.
* `DB_DEMO_ADMIN_PW` [DEFAULT null] if set, the generated administrator-account gets assigned this password instead of a random one
* `DB_STRICT_SCHEMA_VERSION` [DEFAULT 0] whether to enforce matching database schema version with respect to currently installed `dcm-database`

### Users
* `REQUIRE_USER_ACTIVATION` [DEFAULT 1]: whether new users are required to set password before login

### Templates
* `HOTFOLDER_SRC` [DEFAULT '[]']: array of hotfolders as JSON or path to a (UTF-8 encoded) JSON-file; every entry of that array needs to have the following signature
  ```json
  {
    "id": "<unique id>",
    "mount": "<mount path of the hotfolder>",
    "name": "<display name of hotfolder>",
    "description": "<(optional) description for hotfolder>"
  }
  ```

  Note that the `"id"` is passed on to the [Import Module](https://github.com/lzv-nrw/dcm-import-module) during execution of a job with hotfolder-import.
  Hence, the Import Module requires a matching definition of hotfolders.
* `ARCHIVES_SRC` [DEFAULT '[]']: array of archive configurations as JSON or path to a (UTF-8 encoded) JSON-file; every entry of that array needs to have the following signature
  ```json
  {
    "id": "<unique id>",
    "name": "<display name for archive>",
    "description": "<(optional) description for archive>",
    "type": "<archive type (see below)>",
    "details": { /* type-specific additional settings */ }
  }
  ```

  Currently, the following archive types are available:
  * `"rosetta-rest-api-v0"`:
    * [ExLibris Rosetta REST-API](https://developers.exlibrisgroup.com/rosetta/apis/rest-apis/)
    * schema for the accompanying `"details"` object:
      ```json
      {
        "url": "<base-url for api-requests>",
        "materialFlow": "<pre-configured material-flow id>",
        "producer": "<pre-configured producer id>",
        "authfile": "<path to a basic-auth header file>",
        "basicAuth": "<basic auth header, i.e. 'Authorization: Basic ...'>",
        "proxy": { /* JSON object; this is passed to the requests library*/ }
      }
      ```

### Scheduling
* `SCHEDULING_CONTROLS_API` [DEFAULT 0] whether the scheduling-api is available
* `SCHEDULING_AT_STARTUP` [DEFAULT 1] whether job scheduling-loop is active at startup
* `SCHEDULING_TIMEZONE` [DEFAULT null] timezone used during scheduling; if null, uses the system default (use python3's `import zoneinfo; zoneinfo.available_timezones()` to view all available options)

### Job Execution (Job Controller)
* `JOB_PROCESSOR_TIMEOUT` [DEFAULT 30] service timeout duration in seconds
* `JOB_PROCESSOR_HOST` [DEFAULT http://localhost:8087] Job Processor host address
* `JOB_PROCESSOR_POLL_INTERVAL` [DEFAULT 1.0] Job Processor polling interval

### Artifact-API
* `ARTIFACT_COMPRESSION` [DEFAULT 0]: whether to use compression while bundling job-artifacts
* `ARTIFACT_BUNDLE_DESTINATION` [DEFAULT "bundles"]: output destination for artifact-bundles
* `ARTIFACT_FILE_MAX_SIZE` [DEFAULT 0]: maximum allowed size of individual file (before compression) in B; a value of zero corresponds to no limit
* `ARTIFACT_BUNDLE_MAX_SIZE` [DEFAULT 0]: maximum allowed size of bundle (before compression) in B; a value of zero corresponds to no limit
* `ARTIFACT_SOURCES` [DEFAULT '["ie", "ip", "pip", "sip"]']: accepted source-directories for bundle-targets as a JSON-array of strings; relative to shared file storage (a target in a bundle-request needs to be located inside one of these source directories to be valid)

Additionally this service provides environment options for
* `BaseConfig`,
* `OrchestratedAppConfig`,
* `FSConfig`, and
* `DBConfig`

as listed [here](https://github.com/lzv-nrw/dcm-common#app-configuration).

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
* Roman Kudinov