FROM python:3.12-slim
RUN pip install --no-cache-dir boto3==1.35.*
COPY init_aws.py /init_aws.py
CMD ["python", "/init_aws.py"]
