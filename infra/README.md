This repository contains Terraform templates for deploying infrastructure on AWS. Follow the steps below to set up your workspace, customize the templates, and deploy your infrastructure.

## Prerequisites

Before you begin, ensure you have the following tools installed:

- [Terraform](https://www.terraform.io/downloads.html) (v1.0 or later)
- [AWS CLI](https://aws.amazon.com/cli/) (configured with appropriate AWS credentials)
- [Git](https://git-scm.com/)

## Repository Structure

- `main.tf`: Main configuration file for Terraform resources
- `variables.tf`: Variables used in the Terraform configuration
- `outputs.tf`: Outputs for the Terraform configuration
- `provider.tf`: Configuration for the AWS provider
- `README.md`: This file

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/your-repo.git
cd your-repo
```

### 2. Configure AWS Credentials

Ensure your AWS CLI is configured with the necessary credentials. You can do this by running:

```bash
aws configure
```

Follow the prompts to enter your AWS Access Key, Secret Key, region, and output format.

### 3. Initialize Terraform

Initialize your Terraform workspace. This will download the necessary provider plugins.

```bash
terraform init
```

### 4. Customize Variables

Edit the variables.tf file to customize the variables according to your requirements. For example:

```bash
variable "aws_region" {
  description = "The AWS region to deploy in"
  default     = "eu-west-1"
}
```

### 5. Plan the Deployment

Run the following command to see what Terraform will do before actually applying the changes:

```bash
terraform plan
```

Review the output to ensure that the resources to be created/modified/deleted match your expectations.

### 6. Apply the Deployment

Apply the Terraform configuration to deploy the resources:

```bash
terraform apply
```

Type yes when prompted to confirm the deployment.

### 7. Access the Outputs

After the deployment is complete, you can view the outputs defined in outputs.tf:

```bash
terraform output
```

## Managing Workspaces

Terraform workspaces allow you to manage multiple environments (e.g., dev, staging, production) with a single configuration.

### Create a New Workspace

```bash
terraform workspace new <workspace_name>
```

### List Workspaces

```bash
terraform workspace list
```

### Select a Workspace

```bash
terraform workspace select <workspace_name>
```

### Delete a Workspace

```bash
terraform workspace delete <workspace_name>
```

Note: You cannot delete a workspace if it is currently selected. Switch to another workspace first.

### Cleaning Up

To destroy the resources created by Terraform:

```bash
terraform destroy
```

Type yes when prompted to confirm the destruction.

## Contributing

If you find a bug or have a feature request, please open an issue or submit a pull request. Contributions are welcome!

## License

This project is licensed under the MIT License - see the LICENSE file for details.