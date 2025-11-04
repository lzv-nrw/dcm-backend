from setuptools import setup


setup(
    version="6.0.0",
    name="dcm-backend",
    description="flask app for dcm-backend-containers",
    author="LZV.nrw",
    install_requires=[
        "flask==3.*",
        "requests==2.*",
        "PyYAML==6.*",
        "python-dateutil==2.*",
        "argon2-cffi>=23.1.0,<24",
        "data-plumber-http>=1.0.0,<2",
        "dcm-common[services, db, orchestra]>=4.0.0,<5",
        "dcm-database>=3.0.0,<4",
        "dcm-backend-api>=5.0.0,<6",
        "dcm-job-processor-sdk>=3.0.0,<4",
    ],
    packages=[
        "dcm_backend",
        "dcm_backend.components",
        "dcm_backend.components.archive_controller",
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
