// includes the `serviceAccount:` component as well
output "app_engine_svc_acct_email" {
  value = google_project_iam_member.app_db_client.member
}
