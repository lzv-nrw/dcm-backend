from setuptools import setup

setup(
    version="1.0.1",
    name="dcm-backend",
    description="flask app for dcm-backend-containers",
    author="LZV.nrw",
    install_requires=[
        "flask==3.*",
        "requests==2.*",
        "PyYAML==6.*",
        "schedule==1.*",
        "data-plumber-http>=1.0.0,<2",
        "dcm-common[services, db, orchestration]>=3.11.0,<4",
        "dcm-backend-api>=0.1.0,<1",
        "dcm-job-processor-sdk>=0.1.0,<1",
    ],
    packages=[
        "dcm_backend",
        "dcm_backend.components",
        "dcm_backend.extensions",
        "dcm_backend.models",
        "dcm_backend.views",
    ],
    extras_require={
        "cors": ["Flask-CORS==4"],
    },
    include_package_data=True,
    setuptools_git_versioning={
          "enabled": True,
          "version_file": "VERSION",
          "count_commits_from_version_file": True,
          "dev_template": "{tag}.dev{ccount}",
    },
)
