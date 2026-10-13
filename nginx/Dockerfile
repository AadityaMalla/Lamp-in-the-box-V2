# =============================================================
# Author : Aaditya Malla (CSCI 312 - Group 4)
# File   : nginx/Dockerfile
# Purpose: NGINX web server + reverse proxy image
# =============================================================
FROM nginx:stable-alpine
ENV ROCKET_HOST=rocket
COPY default.conf.template /etc/nginx/templates/default.conf.template
COPY index.html /usr/share/nginx/html/index.html
EXPOSE 80
