module "ow-emblem-interchange" {
  source        = "../src/resource/storage/bucket"
  name          = "ow-emblem-interchange"
  project_id    = var.partner_project_production
  location      = "US-EAST1"
  storage_class = "REGIONAL"
  labels = {
    data = "phi"
  }
  bucket_policy_data = [
    { role = "roles/storage.objectAdmin",
      members = [
        "user:danny.dvinov@cityblock.com",
        "user:jac.joubert@cityblock.com",
        "user:pavel.znoska@cityblock.com",
        "user:jenny.wang@cityblock.com",
        "user:dax@cityblock.com",
        "user:mary.adomshick@oliverwyman.com",
        "user:mary_adomshick_oliver_wyman@cityblock-partners.com"
    ] },
    { role = "roles/storage.admin",
      members = [
        "group:gcp-admins@cityblock.com",
    ] }
  ]
}
