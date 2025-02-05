# Use the official Apache Airflow image as the base
FROM apache/airflow:2.10.4

# Set environment variables for Java and Spark
ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Switch to root user to install dependencies
USER root

# Install necessary dependencies and clean up
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y openjdk-11-jdk wget unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set up Spark environment variables
RUN wget https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz && \
    tar -xvzf spark-3.2.1-bin-hadoop3.2.tgz -C /opt/ && \
    rm spark-3.2.1-bin-hadoop3.2.tgz && \
    mv /opt/spark-3.2.1-bin-hadoop3.2 /opt/spark

# Switch back to airflow user to ensure proper permissions
USER airflow

# Copy the requirements file and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and other necessary files into the container
COPY dags /opt/airflow/dags

# Expose the port for the Airflow web server (optional)
EXPOSE 8080

# Default command to start Airflow
CMD ["bash", "-c", "airflow db init && airflow webserver -p 8080 & airflow scheduler"]
