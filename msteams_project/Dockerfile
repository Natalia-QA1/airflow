# Use the Apache Airflow official image
FROM apache/airflow:2.8.1

# Set the AIRFLOW_VERSION environment variable
ENV AIRFLOW_VERSION=2.8.1

# Set the working directory
WORKDIR /opt/airflow

# Copy the requirements file into the container
COPY requirements.txt .

# Add more verbosity to pip install to see more details in case of failure
RUN pip install --no-cache-dir apache-airflow[amazon,postgres]==${AIRFLOW_VERSION} -r requirements.txt

# Copy the DAGs folder into the container
COPY dags /opt/airflow/dags

# Expose necessary ports (if needed)
EXPOSE 8080

RUN airflow db init

CMD ["airflow", "webserver"]
