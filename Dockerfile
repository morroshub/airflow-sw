# Creacion de la imagen 
# 'docker build -t app:tag .'

# Recarga del contenedor.
# docker run -it -p puerto -v %PWD:/usr/src/apptag
# docker run -it -p 8000:8000 -v %PWD:/usr/src/appnombre

# Uso te derminal tty en docker como root 
#docker exec -u root -t -i container_id /bin/bash

# Combinando lineas de codigo
#docker exec -u root -it -p 8000:8000 -v $(pwd):/usr/src/appnombre CONTAINER_ID /bin/bash


FROM apache/airflow:2.8.1

COPY requirements.txt /requirements.txt


RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

