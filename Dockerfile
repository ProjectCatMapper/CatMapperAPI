FROM tiangolo/uwsgi-nginx-flask:python3.8
RUN apt update && apt install -y bash nano vim 
RUN apt install -y libsodium-dev
COPY ./requirements.txt /var/www/requirements.txt
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install -r /var/www/requirements.txt
