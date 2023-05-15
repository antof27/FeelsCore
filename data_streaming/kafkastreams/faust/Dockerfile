FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["lyrics-faust", "-A", "faust-app", "worker", "-l", "info"]
