FROM python:2.7.12

RUN pip install beautifulsoup4
RUN pip install kafka-python

COPY . /opt/

ENTRYPOINT [ "python", "./opt/main.py" ]
