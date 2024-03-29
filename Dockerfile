FROM python:slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt

RUN apt update
RUN apt install -y imagemagick
RUN sed -i '/disable ghostscript format types/,+6d' /etc/ImageMagick-6/policy.xml

RUN pip3 install -r requirements.txt

COPY . .

CMD ["python", "-u", "/app/main.py"]