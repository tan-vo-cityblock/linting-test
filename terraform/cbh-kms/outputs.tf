// TODO: Move outputs to their respecitve ..kms.tf files and delete this file as it is getting too large.

output "app_yaml_key_name" {
  value       = local.app_yaml_key_name
  description = "key name provided as remote output for use in cbh-secrets storage path and member-service"
  sensitive   = true
}

output "db_password_key_name" {
  value       = local.db_password
  description = "key name provided as remote output for use in cbh-secrets and member-service"
  sensitive   = true
}

output "member_service_ring_staging_name" {
  value       = module.member_service_ring_staging.name
  description = "ring name provided as remote output for use in cbh-secrets storage path and member-service"
  sensitive   = true
}

output "member_service_ring_prod_name" {
  value       = module.member_service_ring_prod.name
  description = "ring name provided as remote output for use in cbh-secrets storage path and member-service"
  sensitive   = true
}

output "elation_ssh_private_key_prod" {
  value = {
    ring_name : module.elation_ring_prod.name
    self_link : module.elation_ssh_private_key_prod.crypto_key_self_link
    key_name : module.elation_ssh_private_key_prod.name
    file_names : {
      id_ed25519_cbh_elation : "id-ed25519-cbh-elation",
      id_ed25519_cbh_elation_64_enc : "id-ed25519-cbh-elation.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

output "elation_ssh_config_prod" {
  value = {
    ring_name : module.elation_ring_prod.name
    self_link : module.elation_ssh_config_prod.crypto_key_self_link
    key_name : module.elation_ssh_config_prod.name
    file_names : {
      ssh_config : "ssh-config",
      ssh_config_64_enc : "ssh-config.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

output "elation_known_hosts_prod" {
  value = {
    ring_name : module.elation_ring_prod.name
    self_link : module.elation_known_hosts_prod.crypto_key_self_link
    key_name : module.elation_known_hosts_prod.name
    file_names : {
      known_hosts : "known-hosts",
      known_hosts_64_enc : "known-hosts.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

output "elation_mysql_config_prod" {
  value = {
    ring_name : module.elation_ring_prod.name
    self_link : module.elation_mysql_config_prod.crypto_key_self_link
    key_name : module.elation_mysql_config_prod.name
    file_names : {
      my_cnf : "my.cnf"
      my_cnf_64_enc : "my.cnf.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

output "ablehealth_user_prod" {
  value = {
    ring_name : module.ablehealth_ring_prod.name
    self_link : module.ablehealth_user_prod.crypto_key_self_link
    key_name : module.ablehealth_user_prod.name
    file_names : {
      user : "user",
      user_64_enc : "user.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

output "ablehealth_pwd_prod" {
  value = {
    ring_name : module.ablehealth_ring_prod.name
    self_link : module.ablehealth_pwd_prod.crypto_key_self_link
    key_name : module.ablehealth_pwd_prod.name
    file_names : {
      password : "password",
      password_64_enc : "password.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

output "ablehealth_s3_creds_prod" {
  value = {
    ring_name : module.ablehealth_ring_prod.name
    self_link : module.ablehealth_s3_creds.crypto_key_self_link
    key_name : module.ablehealth_s3_creds.name
    file_names : {
      credentials : "credentials",
      credentials_64_enc : "credentials.64.enc"
    }
  }
  description = "Map of key attributes containing self link and name"
  sensitive   = true
}

output "commons_github_creds" {
  value = {
    ring_name : module.commons_github_ring.name
    self_link : module.commons_github_creds.crypto_key_self_link
    key_name : module.commons_github_creds.name
    file_names : {
      commons_github_token : "commons_github_token",
      commons_github_token_64_enc : "commons_github_token.64.enc"
    }
  }

  description = "Map of key attributes containing self link and name"
  sensitive   = true
}
