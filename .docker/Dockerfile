FROM node:14.15.3-buster

# Convenience
RUN apt-get update \
  && apt-get -y install vim sudo vnc4server less wget git locales \
  && rm -rf /var/cache/apt/*

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen
ENV LC_ALL en_US.UTF-8
RUN echo "node ALL=NOPASSWD: ALL" > /etc/sudoers.d/60-user

COPY px /usr/bin/

USER node

RUN echo 'set -o vi' >> ~/.bashrc
ENV PATH ./node_modules/.bin:${PATH}

RUN sudo apt-get update \
  && sudo apt-get -y install postgresql-client \
  && sudo rm -rf /var/cache/apt/*

RUN sudo wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.4/dumb-init_1.2.4_x86_64
RUN sudo chmod +x /usr/local/bin/dumb-init
