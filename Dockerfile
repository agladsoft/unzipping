FROM python:3.8

ARG XL_IDP_PATH_DOCKER

ENV XL_IDP_PATH_DOCKER=$XL_IDP_PATH_DOCKER

RUN apt update -y && apt upgrade -y && chmod -R 777 $XL_IDP_PATH_DOCKER

RUN wget http://ftp.us.debian.org/debian/pool/non-free/r/rar/rar_5.5.0-1_amd64.deb  \
    && dpkg -i rar_5.5.0-1_amd64.deb  \
    && apt-get install -f

ADD ./requirements.txt ./requirements.txt

ADD ./unzipping_table.xlsx ./unzipping_table.xlsx

RUN pip install -r requirements.txt