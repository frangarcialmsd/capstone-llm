FROM public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON=python3 \
	PYTHONUNBUFFERED=1
WORKDIR /opt/spark/work-dir

COPY pyproject.toml README.md requirements.txt ./
COPY src ./src

RUN python3 -m pip install --no-cache-dir --upgrade pip \
	&& sed '/^-e /d' requirements.txt > /tmp/requirements.txt \
	&& python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
	&& python3 -m pip install --no-cache-dir --no-deps .

ENTRYPOINT ["python3", "-m", "capstonellm.tasks.clean"]
