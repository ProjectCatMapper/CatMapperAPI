FROM tiangolo/uwsgi-nginx-flask:python3.8-alpine
RUN apk --update add bash nano vim build-base
COPY ./requirements.txt /var/www/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /var/www/requirements.txt
