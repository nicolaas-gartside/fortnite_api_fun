FROM python:3.9.13
WORKDIR /test_script
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .