module "elation_mirror_prod_secret_manager_secret" {
  source           = "../src/resource/secret_manager/secret"
  secret_id        = "elation_mirror_prod"
  secret_accessors = ["serviceAccount:${module.elation_worker_svc_acct.email}"]
}

module "elation_private_ssh_key_prod_secret_manager_secret" {
  source           = "../src/resource/secret_manager/secret"
  secret_id        = "elation_private_ssh_key_prod"
  secret_accessors = ["serviceAccount:${module.elation_worker_svc_acct.email}"]
}

module "prod_elation_api_creds_secret" {
  source           = "../src/resource/secret_manager/secret"
  secret_id        = "prod-elation-api-creds"
  secret_accessors = ["serviceAccount:${module.prod_healthgorilla_worker_svc_acct.email}"]
}

module "dev_elation_api_creds_secret" {
  source           = "../src/resource/secret_manager/secret"
  secret_id        = "dev-elation-api-creds"
  secret_accessors = ["serviceAccount:${module.dev_healthgorilla_worker_svc_acct.email}"]
}


# aws credentials for pushing to elation s3 / secret for prod elation pipelines
module "elation_aws_secrets" {
  source           = "../src/resource/secret_manager/secret"
  secret_id        = "elation-aws-secrets"
  secret_accessors = ["serviceAccount:${module.elation_worker_svc_acct.email}"]
}

module "elation_aws_secrets_k8s_secret" {
  providers = {
    kubernetes = "kubernetes.cityblock-orchestration-prod"
  }
  source     = "../src/custom/secret_manager_secret_to_k8_secret"
  secret_id  = module.elation_aws_secrets.secret_id
  secret_key = "credentials"
}
