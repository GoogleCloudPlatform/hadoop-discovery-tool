# Name of the Project

One/two paragraph summary of the use case and what problem we are trying to solve.

* Industry: `Manufacturing Industry`, `Automobile Industry`, etc.
* ML Domain: `NLP`, `Computer Vision`, etc.

## Problem Statement

Detailed parargraph on the problem statement - max one paragraph

## Solution

Detailed description of the solution - max one paragraph

## Technology

High level understanding of the technologies used in the solution

### ML Techniques

* Linear Regression - For predicting temperature
* RNN - For neural networks

### Programming languages

* Python 2.7 - For Machine Learning
* Python 2.7 (Flask) - For creating backend API and integration with UI
* Angular 6 - For UI development and front-end data processing
* Google Oauth2 - For API authentication

### Infrastructure (training and Deployment)

* Google Compute Engine
    * 1 VM n1-standard
    * 1 GPU P80
* Cloud Pub/Sub


## Setup
Step by step guide to setup the application

```bash
git clone https://gitlab.qdatalabs.com/devops/boilerplate.git
```

Along with code snippets that needs to be run

```bash
bash ./run.sh
```

And nesessory configurtions to be changed for different environments

```yaml
ENVIRONMENT
  - GOOGLE_APPLICATION_CREDENTIALS: <PATH-TO-SERIVCE-ACCOUNT-FILE>
  - PROJECT_ID: <PROJECT-ID-OF-GCP-PROJECT>
```

Some Important points or warnings should be highlighted

> Take care that the service account key file is not commited and pushed to git

> Do not explain in-detail architecture and case studies here. Create wiki pages for that.

Also highlighting `some words` can be done in this manner.

## Training
Details for what kind os dataset is required for traiing

```
along with the code execution scripts
```

### Deployment
Details of what infrastructure is used to deploy the application along with the scripts to deploy the application with config

```bash
gcloud app deploy
```

## Testing
Detailed guide on how to run unit test cases 

```bash
python run_unit_tests.py
```
This should produce results in the following format

```
Ran 10/10 tests successfully. Test Successful
```

And end to end test cases
```bash
python run_integrity_tests.py
```

```
Ran 3/5 tests successfully. Test Failed
```