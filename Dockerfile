FROM python:3.12-buster

LABEL maintainer="laercio.serra@gmail.com"
LABEL version="1.0"
LABEL description="data of fire challenge"

WORKDIR /data_of_fire

# Copy all files from the build context (should be project root)
COPY . .

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --trusted-host pypi.python.org -r requirements.txt

# Make all .sh and .py files executable
RUN chmod a+x *.sh || true && find . -name "*.py" -exec chmod a+x {} +

CMD ["bash", "start_app.sh"]