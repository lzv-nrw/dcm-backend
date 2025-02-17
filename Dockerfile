FROM python:3.10-alpine

# setup for all users
RUN umask 022

# set working directory
WORKDIR /app

# copy entire directory into container
COPY . /app/dcm-backend
# move AppConfig
RUN mv /app/dcm-backend/app.py /app/app.py

# install app package
RUN pip install --upgrade \
    --extra-index-url https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple \
    "/app/dcm-backend/[cors]"
RUN rm -r dcm-backend/

# install wsgi server
RUN pip install gunicorn

# add and set default user
RUN adduser -u 303 -S dcm -G users
USER dcm

# define startup
ENV WEB_CONCURRENCY=5
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:80 --workers 1 --threads ${WEB_CONCURRENCY} app:app"]
