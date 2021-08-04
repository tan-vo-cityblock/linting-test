variable "name" {
  type        = "string"
  description = "Name of subscription"
}

variable "topic" {
  type        = "string"
  description = "Topic to subscribe to"
}

variable "dead_letter_topic_id" {
  type        = string
  description = "Dead letter topic resource"
  default     = null
}

variable "project" {
  type        = "string"
  description = "Project for pubsub subscription"
}

variable "ack_deadline_seconds" {
  type        = number
  description = "The maximum time after a subscriber receives a message before the subscriber should acknowledge the message"
  default     = 20
}

variable "message_retention_duration" {
  type        = "string"
  description = <<EOF
    How long to retain unacknowledged messages in backlog, from the moment a message is published. Cannot be more than 7
    days (604800s) or less than 10 minutes (600s).
  EOF
  default     = null
}

variable "max_delivery_attempts" {
  type        = number
  description = "How many times to attempt to send a message before sending it to the dead letter queue"
  default     = 5
}

variable "push_endpoint" {
  type        = "string"
  description = <<EOF
    URL locating the endpoint to which messages should be pushed.
    If push delivery is used with this subscription, this field is passed to the push_config block to configure it.
    An empty pushConfig signifies that the subscriber will pull and ack messages using API methods. Structure is documented below.
  EOF
  default     = null
}

variable "subscription_ttl_expiration" {
  type        = "string"
  description = <<EOF
    How long for subscription to expire without pull/push activity (ex. "300000.5s"). "" sets the expiration to "never
    expires. See https://www.terraform.io/docs/providers/google/r/pubsub_subscription.html#expiration_policy for more
    information.
  EOF
  default     = ""
}

variable "labels" {
  type = map(string)
  default = {}
  description = "Map of labels to apply to the Pub/Sub subscription resource"
}

variable "default_oidc_token_email" {
  type = "string"
  default = null
  description = "Service account email to be used for generating the OIDC token"
}

module "subscription_project_ref" {
  source     = "../../../data/project"
  project_id = var.project
}

resource "google_pubsub_subscription" "subscription" {
  name    = var.name
  topic   = var.topic
  project = var.project

  ack_deadline_seconds       = var.ack_deadline_seconds
  message_retention_duration = var.message_retention_duration

  labels = var.labels

  dynamic "push_config" {
    for_each = var.push_endpoint == null ? [] : [var.push_endpoint]
    content {
      push_endpoint = push_config.value
      attributes = {
        x-goog-version = "v1"
      }
      dynamic "oidc_token" {
        for_each = var.default_oidc_token_email == null ? [] : [
          var.default_oidc_token_email]
        content {
          service_account_email = oidc_token.value
        }
      }
    }
  }

  dynamic "dead_letter_policy" {
    for_each = var.dead_letter_topic_id == null ? [] : [var.dead_letter_topic_id]
    content {
      dead_letter_topic     = var.dead_letter_topic_id
      max_delivery_attempts = var.max_delivery_attempts
    }
  }

  expiration_policy {
    ttl = var.subscription_ttl_expiration
  }

  retry_policy {
    minimum_backoff = "15s"
  }
}

variable "subscription_policy_data" {
  type        = list(object({ role = string, members = list(string) }))
  description = <<EOF
    Authoritative for a given role. Updates the IAM policy to grant a role to a list of members.
    Other roles within the IAM policy for the topic are preserved.
    See for more details on roles: https://cloud.google.com/pubsub/docs/access-control#overview
  EOF
}

data "google_iam_policy" "subscription_policy_data" {
  // ensure that default subscriber is added if subscriber role does not yet exist in bindings
  binding {
    members = ["serviceAccount:${module.subscription_project_ref.default_pubsub_service_account_email}"]
    role = "roles/pubsub.subscriber"
  }
  // if subscriber role already exists in policy data, merely append it to member list to add it; will override above
  dynamic "binding" {
    for_each = var.subscription_policy_data
    content {
      role    = binding.value.role
      members = binding.value.role == "roles/pubsub.subscriber" ? concat(binding.value.members, ["serviceAccount:${module.subscription_project_ref.default_pubsub_service_account_email}"]) : binding.value.members
    }
  }
}

resource "google_pubsub_subscription_iam_policy" "subscription_policy" {
  project      = google_pubsub_subscription.subscription.project
  subscription = google_pubsub_subscription.subscription.name
  policy_data  = data.google_iam_policy.subscription_policy_data.policy_data
}
