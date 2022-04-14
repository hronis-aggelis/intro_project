# sam-app

This project contains source code and supporting files for a serverless application that you can deploy with the SAM CLI. It includes the following files and folders.

- sam_app - Code for the application's Lambda function.
- template.yaml - A template that defines the application's AWS resources.

The application uses several AWS resources, including Lambda function, API Gateway, S3, DynamoDb and systems manager. These resources are defined in the `template.yaml` file in this project. 


To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* [Python 3 installed](https://www.python.org/downloads/)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)