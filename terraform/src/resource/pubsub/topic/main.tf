output "name" {
  value = google_pubsub_topic.topic.name
}

output "id" {
  value = google_pubsub_topic.topic.id
}

output "topic_policy_data" {
  value       = data.google_iam_policy.topic_policy_data.policy_data
  description = "Policy data for use by other topics, namely, dead letter topics"
}

variable "name" {
  type        = "string"
  description = "Name of topic"
}

variable "project" {
  type        = "string"
  description = "Project for pubsub subscription"
}

variable "labels" {
  type        = map(string)
  default     = {}
  description = "Map of labels to apply to the Pub/Sub topic resource"
}


resource "google_pubsub_topic" "topic" {
  name    = var.name
  project = var.project
  labels  = var.labels
}

variable "topic_policy_data" {
  type        = list(object({ role = string, members = list(string) }))
  description = <<EOF
    Authoritative for a given role. Updates the IAM policy to grant a role to a list of members.
    Other roles within the IAM policy for the topic are preserved.
    See for more details on roles: https://cloud.google.com/pubsub/docs/access-control#overview
  EOF
}

data "google_iam_policy" "topic_policy_data" {

  dynamic "binding" {
    for_each = var.topic_policy_data
    content {
      role    = binding.value.role
      members = binding.value.members
    }
  }
}

resource "google_pubsub_topic_iam_policy" "policy" {
  project     = google_pubsub_topic.topic.project
  topic       = google_pubsub_topic.topic.name
  policy_data = data.google_iam_policy.topic_policy_data.policy_data
}
