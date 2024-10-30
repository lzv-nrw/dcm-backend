# dcm-backend

The 'DCM Backend'-service provides backend functionalities to the DCM.

Currently only the `ArchiveController` component is available that offers the following functionalities ([see below](#usage-of-the-archive-controller)):
* triggering a new deposit activity into Rosetta (`POST` request), and
* retrieving the status of an existing deposit activity in Rosetta (`GET` request).

## Helper script to generate the file with the Authorization HTTP header

The file `convert_credentials.sh` contains a helper script to generate
the Authorization HTTP header for the requests. The path to the output file
can optionally be given as an input parameter, with default being
`~/.rosetta/rosetta_auth`.
The user is prompted to provide the institution code, the username,
and the password for the used Rosetta instance.

If the file `rosetta_auth` already exists, the user is prompted whether
to allow overwriting it.

For testing the script result, the environment variable `ARCHIVE_API_BASE_URL`
has to be set and the following curl command can be executed:

```
curl -X GET "${ARCHIVE_API_BASE_URL}/rest/v0/deposits" -H @<path-to-header-file> -H 'accept: application/json'
```

## Usage of the Archive Controller
Example for a `GET` request, which retrieves the status of a deposit activity:
```python
from dcm_backend.config import AppConfig
from dcm_backend.components import ArchiveController
archive_controller = ArchiveController(
    auth=AppConfig.ROSETTA_AUTH_FILE,
    url=AppConfig.ARCHIVE_API_BASE_URL
)
archive_controller.get_deposit("0000")
```

Example for a `POST` request, which triggers a new deposit activity:
```python
from dcm_backend.config import AppConfig
from dcm_backend.components import ArchiveController
archive_controller = ArchiveController(
    auth=AppConfig.ROSETTA_AUTH_FILE,
    url=AppConfig.ARCHIVE_API_BASE_URL
)
archive_controller.post_deposit(
    subdirectory="dir",
    producer="1234",
    material_flow="1234"
)
```

## Usage of the Scheduler
Example usage for printing the time every other second:

```python
from time import sleep

from dcm_common.util import now

from dcm_backend.models import JobConfig, Repeat, Schedule, TimeUnit
from dcm_backend.components import Scheduler


def f(config):
    print(now())

scheduler = Scheduler(job_cmd=f)

job_config_id = "0000"
job_config = JobConfig(
    id_=job_config_id,
    last_modified=now(),
    job={},
    schedule=Schedule(
        active=True,
        repeat=Repeat(unit=TimeUnit.SECOND, interval=2)
    )
)
scheduler.schedule(job_config)

for i in range(10):
    scheduler.run_pending()
    sleep(1)

scheduler.cancel(job_config_id)
for i in range(5):
    scheduler.run_pending()
    sleep(1)
```

## Run locally
Running in a `venv` is recommended.

To test the app locally,
1. install with
   ```
   pip install .
   ```
1. Configure service environment to your needs ([see here](#environmentconfiguration)).
1. run as
   ```
   flask run --port=8080
   ```
1. use either command line tools like `curl`,
   ```
   curl -X 'POST' \
     'http://localhost:8080/ingest' \
     -H 'accept: application/json' \
     -H 'Content-Type: application/json' \
     -d '{
    "ingest": {
        "archive_identifier": "rosetta",
        "rosetta": {
         "subdir": "test_dir"
        }
    }
   }'
   ```
   or a gui like [swagger-ui](https://github.com/lzv-nrw/dcm-backend-api/-/blob/dev/dcm_backend_api/openapi.yaml?ref_type=heads) (see sibling package [`dcm-backend-api`](https://github.com/lzv-nrw/dcm-backend-api)) to submit jobs


## Run with Docker
### Container setup
Use the `compose.yml` to start the `DCM Backend`-container as a service:
```
docker compose up
```
(to rebuild run `docker compose build`).

A Swagger UI is hosted at
```
http://localhost/docs
```
while (by-default) the app listens to port `8080`.

Afterwards, stop the process for example with `Ctrl`+`C` and enter `docker compose down`.

If the file `rosetta_auth` is not stored in the default path (`~/.rosetta/rosetta_auth`),
the path of the secret `rosetta-auth` of the `compose.yml` should be corrected
before building the container.

The build process requires authentication with `zivgitlab.uni-muenster.de` in order to gain access to the required python dependencies.
The Dockerfiles are setup to use the information from `~/.netrc` for this authentication (a gitlab api-token is required).

## Configuration for Rosetta test-instance
The Archive Controller should be configured to work with the Rosetta test system (hosted by the hbz).
The Rosetta instance (API) can (currently) be accessed by using the regular credentials also used for the web-interface.
Depending on whether the 'DCM Backend'-service is run in docker or locally, the configuration has to be applied to either the `compose.yml` or in the environment where `flask run` is executed.

### Rosetta API
In order to make requests to the Rosetta API, the credentials have to be given in the correct format, i.e., as an HTTP-Basic Authentication header. [See the helper script to generate the file with the Authorization HTTP header](#helper-script-to-generate-the-file-with-the-authorization-http-header) 

### Environment
Set the following environment variables:
  * `ARCHIVE_API_BASE_URL`
  * `ROSETTA_PRODUCER`
  * `ROSETTA_MATERIAL_FLOW`

This repository provides the environment-file `rosettaapi.env` that can be used to load the correct settings.
Simply add `env_file: rosettaapi.env` to the service definition in the `compose.yml` (note that the `environment`-block takes precedence over `env_file`) or run `export $(xargs < rosettaapi.env)` (locally).

## Tests
Install additional dependencies from `dev-requirements.txt`.
Run unit-tests with
```
pytest -v -s --cov dcm_backend
```

## Environment/Configuration
Service-specific environment variables are

### Database
* `CONFIGURATION_DATABASE_ADAPTER` [DEFAULT "native"]: which adapter-type to use for the configuration database
* `CONFIGURATION_DATABASE_SETTINGS` [DEFAULT {"backend": "memory"}]: JSON object containing the relevant information for initializing the adapter
* `REPORT_DATABASE_ADAPTER` [DEFAULT "native"]: which adapter-type to use for the job report database
* `REPORT_DATABASE_SETTINGS` [DEFAULT {"backend": "memory"}]: JSON object containing the relevant information for initializing the adapter

### SCHEDULING
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

as listed [here](https://github.com/lzv-nrw/dcm-common/-/tree/dev?ref_type=heads#app-configuration).

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
