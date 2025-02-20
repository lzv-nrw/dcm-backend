# Digital Curation Manager - Backend

The 'DCM Backend'-API provides functionality to
* trigger an ingest in the archive-system,
* collect the current ingest-status,
* manage job configurations,
* control job execution,
* authenticate local users, and
* manage user configurations.

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
       "archive_identifier": "rosetta",
       "rosetta": {
         "subdir": "2468edf8-6706-4ff0-bd03-04512d082c28",
         "producer": "12345678",
         "material_flow": "12345678"
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

### Database
* `JOB_CONFIGURATION_DATABASE_ADAPTER` [DEFAULT "native"]: which adapter-type to use for the job configuration database
* `JOB_CONFIGURATION_DATABASE_SETTINGS` [DEFAULT {"backend": "memory"}]: JSON object containing the relevant information for initializing the job configuration database adapter
* `REPORT_DATABASE_ADAPTER` [DEFAULT "native"]: which adapter-type to use for the job report database
* `REPORT_DATABASE_SETTINGS` [DEFAULT {"backend": "memory"}]: JSON object containing the relevant information for initializing the job report database adapter
* `USER_CONFIGURATION_DATABASE_ADAPTER` [DEFAULT "native"]: which adapter-type to use for the user configuration database
* `USER_CONFIGURATION_DATABASE_SETTINGS` [DEFAULT {"backend": "memory"}]: JSON object containing the relevant information for initializing the user configuration database adapter

### Users
* `CREATE_DEMO_USERS` [DEFAULT 0]: whether demo users are created at startup
* `REQUIRE_USER_ACTIVATION` [DEFAULT 1]: whether new users are required to set password before login
* `USER_ACTIVATION_URL_FMT` [DEFAULT "ERROR: ..."]: python format-string containing `..{password}..` as key; used to format user-activation urls

  Two demo users are created. User 'Einstein' with password 'relativity' and user 'Curie' with password 'radioactivity'.

### Scheduling
* `SCHEDULING_CONTROLS_API` [DEFAULT 0] whether the scheduling-api is available
* `SCHEDULING_AT_STARTUP` [DEFAULT 1] whether job scheduling-loop is active at startup
* `SCHEDULING_INTERVAL` [DEFAULT 1.0] loop interval in seconds

### Job Execution (Job Controller)
* `JOB_PROCESSOR_TIMEOUT` [DEFAULT 30] service timeout duration in seconds
* `JOB_PROCESSOR_HOST` [DEFAULT http://localhost:8086] Job Processor host address
* `JOB_PROCESSOR_POLL_INTERVAL` [DEFAULT 1.0] Job Processor polling interval

### Ingest (Archive Controller)
* `ROSETTA_AUTH_FILE` [DEFAULT "~/.rosetta/rosetta_auth"]: path to file with the Authorization HTTP header for all requests
* `ROSETTA_MATERIAL_FLOW`: ID of the Material Flow used for deposit activities
* `ROSETTA_PRODUCER`: Producer ID of deposit activities
* `ARCHIVE_API_BASE_URL` [DEFAULT "https://lzv-test.hbz-nrw.de"]: url to the archive instance
* `ARCHIVE_API_PROXY` [DEFAULT null]: JSON object containing a mapping of protocol name and corresponding proxy-address

Additionally this service provides environment options for
* `BaseConfig` and
* `OrchestratedAppConfig`

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