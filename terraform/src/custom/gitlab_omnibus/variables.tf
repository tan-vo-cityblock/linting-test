variable "project_id" {
  type        = string
  description = "The project to create the GitLab instance under"
}

variable "data_volume" {
  type        = string
  description = "A storage volume for storing your GitLab data"
  default     = "default"
}

variable "image" {
  type        = string
  description = "The image to use for the instance"
  default     = "ubuntu-1604-xenial-v20191113"
}

variable "machine_type" {
  type        = string
  description = "A machine type for your compute instance"
  default     = "n1-standard-4"
}

variable "network" {
  type        = string
  description = "The network for the instance to live on"
  default     = "default"
}

variable "public_ports_ssl" {
  type        = list(string)
  description = "A list of ports that need to be opened for GitLab to work"
  default     = ["80", "443", "22"]
}

variable "region" {
  type        = string
  description = "The region this all lives in."
  default     = "us-east4"
}

variable "zone" {
  type        = string
  description = "The zone to deploy the machine to"
  default     = "us-east4-a"
}

variable "instance_name" {
  type        = string
  description = "The name of the instance to use"
}

variable "dns_name" {
  type        = string
  description = "The DNS name of the GitLab instance."
  default     = "gitlab.cityblock.com."
}

variable "dns_zone" {
  type        = string
  description = "The name of the DNS zone in Google Cloud that the DNS name should go under"
  default     = "cityblock"
}

variable "dns_project" {
  type        = string
  description = "Project containing DNS Zone and records"
}
