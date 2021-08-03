package cityblock.member.resolution.models

/* A class meant to be used as a Label for the NodeEntity. The Label has a one to many relationship with NodeEntity */
case class Label[T](identifier: T, pseudo: Boolean)
