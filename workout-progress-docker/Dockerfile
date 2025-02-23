FROM python:3.10-slim

# install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    vim \
    curl \
    wget \
    procps \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libffi-dev \
    liblzma-dev \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# copy the requirements files into the container
COPY requirements_dbt.txt requirements_airflow.txt requirements_streamlit.txt /root/

# set HOME env var
ENV HOME /root

# create virtual environments for each tool
RUN python -m venv /opt/venv_pydbt && \
    /opt/venv_pydbt/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv_pydbt/bin/pip install --no-cache-dir -r /root/requirements_dbt.txt && \
    \
    python -m venv /opt/venv_pyairflow && \
    /opt/venv_pyairflow/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv_pyairflow/bin/pip install --no-cache-dir -r /root/requirements_airflow.txt && \
    \
    python -m venv /opt/venv_pystreamlit && \
    /opt/venv_pystreamlit/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv_pystreamlit/bin/pip install --no-cache-dir -r /root/requirements_streamlit.txt

# copy required folders
COPY etls /root/etls
COPY dags /root/dags
COPY dashboard /root/dashboard

# copy entrypoint script and make it executable
COPY entrypoint.sh /root/
RUN chmod +x /root/entrypoint.sh

# expose the Streamlit port
EXPOSE 8501

# set working directory inside the container
WORKDIR /root/etls

# set the entrypoint
ENTRYPOINT ["/root/entrypoint.sh"]
