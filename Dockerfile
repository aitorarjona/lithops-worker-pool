# Define custom function directory
ARG FUNCTION_DIR="/function"

FROM python:3.8-buster as build-image

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}

# Install the function's dependencies
RUN pip install \
    --target ${FUNCTION_DIR} \
        lithops==2.3.1 \
        awslambdaric \
        boto3 \
        msgpack \
        diskcache


FROM python:3.8-buster

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

# Add Lithops worker pool handler
RUN mkdir lithops_workerpool
COPY lithops_workerpool_lambda.zip ${FUNCTION_DIR}
COPY lambda_entrypoint.py ${FUNCTION_DIR}
RUN unzip -j -d worker lithops_workerpool_lambda.zip && rm lithops_workerpool_lambda.zip

# Put your dependencies here, using RUN pip install... or RUN apt install...

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "lambda_entrypoint.lambda_handler" ]