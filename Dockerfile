FROM python:3.8

ARG XL_IDP_PATH_DOCKER

ENV XL_IDP_PATH_DOCKER=$XL_IDP_PATH_DOCKER

RUN apt update -y && apt upgrade -y && apt install unrar -y && chmod -R 777 $XL_IDP_PATH_DOCKER

ADD ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt