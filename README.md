# Serverless worker pool with Lithops

Implementation of a worker pool with a job queue and stateful workers on Lithops.

![diagram](https://user-images.githubusercontent.com/33722759/116995522-745c8300-acda-11eb-8f5a-7702a4d657eb.png)

The workers are long-lived functions that consume jobs from a job queue (Redis list). The driver only invokes the functions once,
and subsequent `apply_map` generate individual jobs that are put in the queue. This is useful when the map function is fine-grained
and multiple `map` calls are done in a short time. The overhead from invocation is replaced by the overhead of appending the
jobs to the queue (which is much lower), and functions can benefit from the worker's cache to reuse partial data. Also, 
the *straggler* functions effects are avoided. 

> **DISCLAIMER**: This work is experimental, bugs are expected. Current implementation only supports **AWS Lambda** backend. 
> It is **recommended** to set the function timeout to a low value to avoid having bugged blocked workers that could generate a large and expensive Lambda billing.

## Setup

Change `$ACCOUND_ID` and `$REGION` with your AWS account ID and the region where to upload the image (e.g. `ACCOUNT_ID=123456789012`, `REGION=us-east-1`)

1. Create the container runtime image:
```
zip lithops_workerpool_lambda.zip lithops_workerpool/worker/*
docker build . -t $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/lithops-workerpool:latest
rm lithops_workerpool_lambda.zip
```

2. Login to your AWS CLI:

```
aws configure
```

3. Create the ECR repository:

```
aws ecr create-repository --repository-name lithops-workerpool --region $REGION
```

4. Get auth token:

```
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com
```

5. Push the container image to the repository:

```
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/lithops-workerpool:latest
```







