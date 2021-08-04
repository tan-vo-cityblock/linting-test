// Add additional firewall rule resources here as needed.

module "services_prod_default_firewall" {
  source     = "../src/custom/default_firewall_rules"
  project_id = module.services_prod_project.project_id
}
