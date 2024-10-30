# base image
FROM python:3.10-alpine

# copy entire directory into container
COPY . /app/dcm-backend
# copy accessories
COPY ./app.py /app/app.py

# set working directory
WORKDIR /app

# python
RUN --mount=type=secret,id=zivgitlab,target=/root/.netrc \
    pip install --upgrade \
    --extra-index-url https://zivgitlab.uni-muenster.de/api/v4/groups/12466/-/packages/pypi/simple \
    "dcm-backend/[cors]"
RUN pip list
RUN rm -r dcm-backend/
ENV ALLOW_CORS=1

# and wsgi server (gunicorn)
RUN pip install gunicorn

# run and expose to local network(machine)
ENTRYPOINT [ "gunicorn" ]
CMD ["--bind", "0.0.0.0:8080", "app:app"]