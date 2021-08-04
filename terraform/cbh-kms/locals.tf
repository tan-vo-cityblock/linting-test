// locals block is for reused values in .tf files that are not tied to resources.
locals {
  app_yaml_key_name    = "app-yaml-key" //TODO: Move this to terraform_admin project when it is made and make available as a remote state output, it will used wherever encrypted keys exist.
  db_password          = "db-password"
  ssh-private-key      = "ssh-private-key"
  ssh-public-key       = "ssh-public-key"
  ssh-config           = "ssh-config"
  known-hosts          = "known-hosts"
  mysql-config         = "mysql-config"
  user                 = "user"
  password             = "password"
  db-service-user      = "db-service-user"
  db-mirror-user       = "db-mirror-user"
  api-key-commons      = "api-key-commons"
  api-key-able         = "api-key-able"
  api-key-elation      = "api-key-elation"
  s3-creds             = "s3-creds"
  login-creds          = "login-creds"
  commons-github-token = "commons-github-token"
}
