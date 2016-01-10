FROM library/node
MAINTAINER Tomasz Jonik <tomasz@hurricane.works>

COPY . /data

WORKDIR /data

EXPOSE 8089

CMD ["npm", "start"]
